from typing import Optional
from sqlglot import exp, parse_one

from benchmark.nl2trans_sql.core.schema_inspector import DatabaseSchemaInspector

class SQL:
    def __init__(self, sql_statement, db_schema=None, dialect='postgres'):
        self.sql_statement = sql_statement
        self.db_schema: Optional[DatabaseSchemaInspector] = db_schema
        self.dialect = dialect

        try:
            self.parsed_sql = parse_one(self.sql_statement, dialect=dialect)
        except Exception as e:
            raise RuntimeError(f'SQL parsing error {str(e)}.')

        if isinstance(self.parsed_sql, exp.Select):
            self._extract_select_sql_info()
        elif isinstance(self.parsed_sql, exp.Insert):
            self._extract_insert_sql_info()
        elif isinstance(self.parsed_sql, exp.Update):
            self._extract_update_sql_info()
        elif isinstance(self.parsed_sql, exp.Delete):
            self._extract_delete_sql_info()
        else:
            raise RuntimeError(f'Unsupported SQL')

    def decompose(self):
        if isinstance(self.parsed_sql, exp.Insert):
            return self._decompose_insert_sql()
        elif isinstance(self.parsed_sql, exp.Update):
            return self._decompose_update_sql()
        elif isinstance(self.parsed_sql, exp.Delete):
            return self._decompose_delete_sql()

    def get_type(self):
        type_map = {
            exp.Select: 'SELECT',
            exp.Update: 'UPDATE',
            exp.Insert: 'INSERT',
            exp.Delete: 'DELETE',
        }

        for cls, name in type_map.items():
            if isinstance(self.parsed_sql, cls):
                return name
        return None

    def to_sql(self):
        return self.parsed_sql.sql(dialect=self.dialect)

    def has_pure_field_sel_sql(self):
        return all([isinstance(select_field, exp.Column) for select_field in self.select])

    def get_pure_field_cond_sql(self):
        where_clause = self.get_flattened_where_clause(self.where)
        conditions = []
        for cond in where_clause:
            if isinstance(cond, exp.Binary) and isinstance(cond.left, exp.Column):
                conditions.append(cond)
        return conditions

    def is_agg_sel_sql(self):
        return len(self.select) == 1 and isinstance(self.select[0], exp.AggFunc)

    def is_limit_sql(self):
        return self.parsed_sql.args.get('limit')

    def is_featuring_any(self, feats):
        return any([self.parsed_sql.args.get(f) for f in feats])

    def is_expr_sel_sql(self):
        return len(self.select) == 1 and isinstance(self.select[0], exp.Binary) and \
            not len(list(self.select[0].find_all(exp.AggFunc)))

    def is_group_by_sql(self):
        return 'group' in self.parsed_sql.args

    def is_counting_sql(self):
        return any([len(list(select_field.find_all(exp.Count))) for select_field in self.select])

    def get_source_tables(self):
        return [source for source in self.source if isinstance(source, exp.Table)]

    @staticmethod
    def get_flattened_where_clause(where_expr):
        where_clauses = []

        def _recursive(expr):
            if isinstance(expr, exp.And):
                _recursive(expr.left)
                _recursive(expr.right)
            else:
                where_clauses.append(expr)

        _recursive(where_expr)
        return where_clauses

    def extract_all_tables(self):
        tables = self.parsed_sql.find_all(exp.Table)
        return set([table.name for table in tables])

    def extract_operate_tables(self):
        if isinstance(self.parsed_sql, exp.Select):
            return [self.get_column_table(col).name for col in self.select]
        elif isinstance(self.parsed_sql, exp.Insert):
            return self.insert_table.name
        elif isinstance(self.parsed_sql, exp.Update):
            return self.update_table.name
        elif isinstance(self.parsed_sql, exp.Delete):
            return self.delete_table.name
        else:
            return None

    def get_column_table(self, expr):
        table_name = ''
        if not isinstance(expr, exp.Column):
            column_exprs = list(expr.find_all(exp.Column))
            if not len(column_exprs):
                return None
            expr = column_exprs[0]

        if expr.table:
            table_name = expr.table
        else:
            for table_iter in self.source:
                if expr.name.lower() in self.db_schema.get_column_names(table_iter.name):
                    table_name = table_iter.name
                    break

        return self.name_2_table[table_name]


    def _extract_select_sql_info(self):
        self.select = []
        self.source = []
        self.joins = []
        self.join_conditions = []
        self.where = None
        self.group = None
        self.order = None
        self.limit = None
        self.offset = None

        self.name_2_table = dict()
        self.table_name_2_alias = dict()

        self._parse_select_elements()

        for table in self.source:
            self._add_alias_mapping(table)

    def _parse_select_elements(self):
        statement = self.parsed_sql

        # --- SELECT ---
        for expr in statement.expressions:
            if isinstance(expr, exp.Alias):
                self.select.append(expr.this)
            else:
                self.select.append(expr)

        # --- FROM ---
        from_expr = statement.args.get('from')
        if from_expr:
            self.source.append(from_expr.this)

        # --- JOIN ---
        join_exprs = statement.args.get('joins', [])
        for join in join_exprs:
            self.source.append(join.this)
            self.joins.append(join)
            self.join_conditions.append(join.args.get('on'))

        # --- WHERE ---
        where_expr = statement.args.get('where')
        if where_expr:
            self.where = where_expr.this

        # --- GROUP ---
        group_expr = statement.args.get('group')
        if group_expr:
            self.group = group_expr.this

        # --- ORDER ---
        order_expr = statement.args.get('order')
        if order_expr:
            self.order = order_expr

        # --- LIMIT ---
        limit_expr = statement.args.get('limit')
        if limit_expr:
            self.limit = limit_expr

        # --- OFFSET ---
        offset_expr = statement.args.get('offset')
        if offset_expr:
            self.offset = offset_expr


    def _add_alias_mapping(self, table_expr):
        table_name = table_expr.name
        alias_name = table_expr.alias
        self.name_2_table[table_name] = table_expr
        if alias_name:
            self.name_2_table[alias_name] = table_expr
            self.table_name_2_alias[table_expr.name] = alias_name

    def _extract_insert_sql_info(self):
        self.insert_table = None
        self.insert_columns = []
        self.insert_values = None

        self._parse_insert_elements()

    def _parse_insert_elements(self):
        statement = self.parsed_sql
        insert_schema = statement.this

        self.insert_table = insert_schema.this
        for column_expr in insert_schema.expressions:
            self.insert_columns.append(column_expr)

        self.insert_values = statement.args.get('expression')


    def _extract_update_sql_info(self):

        self.update_table = None
        self.sets = []
        self.reference = None
        self.where = None
        self.order = None
        self.limit = None
        self.offset = None

        self._parse_update_elements()

    def _parse_update_elements(self):
        statement = self.parsed_sql
        self.update_table = statement.this
        self.sets = statement.args.get('expressions')

        # --- FROM ---
        from_expr = statement.args.get('from')
        if from_expr:
            self.reference = from_expr.this

        # --- WHERE ---
        where_expr = statement.args.get('where')
        if where_expr:
            self.where = where_expr.this

        # --- ORDER ---
        order_expr = statement.args.get('order')
        if order_expr:
            self.order = order_expr

        # --- LIMIT ---
        limit_expr = statement.args.get('limit')
        if limit_expr:
            self.limit = limit_expr

        # --- OFFSET ---
        offset_expr = statement.args.get('offset')
        if offset_expr:
            self.offset = offset_expr

    def _extract_delete_sql_info(self):
        self.delete_table = None
        self.reference = None
        self.where = None
        self.order = None
        self.limit = None
        self.offset = None

        self._parse_delete_elements()

    def _parse_delete_elements(self):
        statement = self.parsed_sql
        self.delete_table = statement.this

        # --- FROM ---
        using_expr = statement.args.get('using')
        if using_expr:
            self.reference = using_expr

        # --- WHERE ---
        where_expr = statement.args.get('where')
        if where_expr:
            self.where = where_expr.this

        # --- ORDER ---
        order_expr = statement.args.get('order')
        if order_expr:
            self.order = order_expr

        # --- LIMIT ---
        limit_expr = statement.args.get('limit')
        if limit_expr:
            self.limit = limit_expr

        # --- OFFSET ---
        offset_expr = statement.args.get('offset')
        if offset_expr:
            self.offset = offset_expr


    def _decompose_insert_sql(self):
        """
        :return: insert_table, column, value
        """
        table = self.insert_table.sql(dialect=self.dialect) if self.insert_table else None
        columns = [column_expr.sql(dialect=self.dialect) for column_expr in self.insert_columns] if self.insert_columns else None

        if isinstance(self.insert_values, exp.Select):
            value = self.insert_values.sql(dialect=self.dialect)
        elif isinstance(self.insert_values, exp.Values):
            value = [[literal_expr.sql(dialect=self.dialect) for literal_expr in expressions] for expressions in self.insert_values.expressions]
        else:
            value = None

        return {
            "type": "insert",
            "table": table.replace('public.',''),
            "columns": columns,
            "values": value
        }

    def _decompose_update_sql(self):
        """
        :return: update_table, sets, condition
        """
        table = self.update_table
        sets = [exp.sql(dialect=self.dialect) for exp in self.sets] if self.sets else None
        if table is None or sets is None:
            return None, None, None

        # generate a select sql to express update condition
        select_field = self.db_schema.get_unique_column(table.name)
        f_sf = select_field if select_field else '*'

        f_src = table.sql(dialect=self.dialect)
        if self.reference:
            f_src += f', {self.reference.sql(dialect=self.dialect)}'

        f_wc = self.where
        f_o = [self.order, self.limit, self.offset]

        _suffix = "{}{}".format(f_wc.sql(dialect=self.dialect),
                                  ' ' + ' '.join([obj.sql(dialect=self.dialect) for obj in [s_obj for s_obj in f_o] if obj is not None]))

        select_sql = ' '.join([obj for obj in [
            f"SELECT {table.alias if table.alias else table.name}.{f_sf}",
            f"FROM {f_src}",
            f"WHERE {_suffix}",
        ] if obj])

        return {
            "type": "update",
            "table": table.name,
            "sets": sets,
            "condition": select_sql
        }

    def _decompose_delete_sql(self):
        """
        :return: delete_table, condition
        """
        table = self.delete_table #self.delete_table.sql(dialect=self.dialect) if self.delete_table else None
        if table is None:
            return None, None, None

        # generate a select sql to express update condition
        select_field = self.db_schema.get_unique_column(table.name)
        f_sf = select_field if select_field else '*'

        f_src = table.sql(dialect=self.dialect)
        if self.reference:
            f_src += f', {self.reference.sql(dialect=self.dialect)}'

        f_wc = self.where
        f_o = [self.order, self.limit, self.offset]

        _suffix = "{}{}".format(f_wc.sql(dialect=self.dialect),
                                ' ' + ' '.join([obj.sql(dialect=self.dialect) for obj in [s_obj for s_obj in f_o] if
                                                obj is not None]))

        select_sql = ' '.join([obj for obj in [
            f"SELECT {table.alias if table.alias else table.name}.{f_sf}",
            f"FROM {f_src}",
            f"WHERE {_suffix}",
        ] if obj])

        return {
            "type": "delete",
            "table": table.name,
            "condition": select_sql
        }