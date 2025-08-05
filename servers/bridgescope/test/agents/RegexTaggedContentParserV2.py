import ast
import json

import json
import re
from typing import Union, Sequence, Optional, List

from agentscope.exception import TagNotFoundError, ResponseParsingError
from agentscope.models import ModelResponse
from agentscope.parsers import RegexTaggedContentParser
from loguru import logger


class RegexTaggedContentParserV2(RegexTaggedContentParser):
    def parse(self, response: ModelResponse) -> ModelResponse:
        """Parse the response text by the regex pattern, and return a dict of
        the content in the parsed field of the response.

        Args:
            response (`ModelResponse`):
                The response to be parsed.

        Returns:
            `ModelResponse`: The response with the parsed field as the parsed
            result.
        """
        assert response.text is not None, "The response text is None."

        matches = re.finditer(
            self.tagged_content_pattern,
            response.text,
            flags=re.DOTALL,
        )

        count = len(list(re.finditer(
            self.tagged_content_pattern,
            response.text,
            flags=re.DOTALL,
        )))

        results = {}
        for match in matches:
            if match.group("name") in results and match.group("name") == "thought":
                raise ResponseParsingError("Duplicate tag found in response.")
            results[match.group("name")] = match.group("content")

        keys_missing = [
            key for key in self.required_keys if key not in results
        ]

        if len(keys_missing) > 0:
            raise TagNotFoundError(
                f"Failed to find tags: {', '.join(keys_missing)}",
                response.text,
            )

        if self.try_parse_json:
            keys_failed = []
            for key in results:
                try:
                    # results[key] = json.loads(results[key])
                    results[key] = ast.literal_eval(results[key])
                except Exception as e:
                    keys_failed.append(key)

            if keys_failed:
                logger.debug(
                    f'Failed to parse JSON for keys: {", ".join(keys_failed)}',
                )

        response.parsed = results
        return response
