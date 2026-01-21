import matplotlib

matplotlib.use('Agg')
import os
import uuid
import warnings
import numpy as np
import matplotlib.pyplot as plt

warnings.filterwarnings("ignore", category=UserWarning, message=".*parsable as floats or dates*")

from mcp.server import FastMCP

mcp = FastMCP()

# Create static directory
STATIC_DIR = "static"
if not os.path.exists(STATIC_DIR):
    os.makedirs(STATIC_DIR)


def _save(plt):
    # Generate a unique filename
    filename = f"plot_{uuid.uuid4()}.png"
    filepath = os.path.join(STATIC_DIR, filename)

    # Save the plot
    plt.savefig(filepath)
    plt.close()

    # Return the URL path
    return f"/static/{filename}"


@mcp.tool()
def create_scatter(x_data, y_data, x_name="", y_name="", classify=None, describe=""):
    """
    Creates and returns a scatter plot.

    Parameters:
    -----------
    x_data : list or array-like
        Data for the x-axis, a one-dimensional list or array representing x-values of all data points.
        Example: [1, 2, 3, 4, 5]

    y_data : list or array-like
        Data for the y-axis, a one-dimensional list or array representing y-values of all data points.
        Example: [10, 20, 30, 40, 50]

    x_name : str, optional
        Label for the x-axis describing what the x-values represent.

    y_name : str, optional
        Label for the y-axis describing what the y-values represent.

    classify : list of str, optional
        Classification labels used to distinguish different groups. Only one group is supported, so this will be used as a single legend label.
        Example: ["Group A"]

    describe : str, optional
        Description or title of the overall chart.

    Returns:
    --------
    str: Path to the generated image.

    Example Usage:
    --------------
    create_scatter(
        x_data=[25, 30, 35, 40],
        y_data=[5000, 6000, 7000, 8000],
        x_name="Age",
        y_name="Income",
        classify=["Resident"],
        describe="Income vs Age of Residents"
    )
    """
    if isinstance(x_data[0], (list, tuple)):
        x_data = [item[0] for item in x_data]

    if isinstance(y_data[0], (list, tuple)):
        y_data = [item[0] for item in y_data]

    plt.figure(figsize=(10, 6))

    # Check if x_data and y_data are one-dimensional
    if not isinstance(x_data, (list, np.ndarray)) or not isinstance(y_data, (list, np.ndarray)):
        raise TypeError("x_data and y_data must be one-dimensional lists or arrays")

    if len(x_data) != len(y_data):
        raise ValueError("x_data and y_data must have the same length")

    if classify and len(classify) != 1:
        raise ValueError("Only a single classification label is supported")

    if classify:
        plt.scatter(x_data, y_data, label=classify[0])
    else:
        plt.scatter(x_data, y_data)

    plt.xlabel(x_name)
    plt.ylabel(y_name)

    if describe:
        plt.title(describe)

    if classify:
        plt.legend()

    plt.tight_layout()
    return _save(plt)


@mcp.tool()
def create_pie(x_data, y_data, x_name="", y_name="", classify=None, describe=""):
    """
    Creates and returns a pie chart showing the proportional distribution of categorical data.

    Parameters:
    -----------
    x_data : list or array-like
        List of category labels, one-dimensional list or array representing categories.
        Example: ["Apple", "Banana", "Orange"]

    y_data : list or array-like
        List of numerical values corresponding to each category.
        Example: [30, 50, 20]

    x_name : str, optional
        Name of the category labels (e.g., "Product Type").

    y_name : str, optional
        Name of the numerical values (e.g., "Sales", "Quantity").

    classify : list of str, optional
        Classification labels (currently unused in pie charts, reserved for future use).

    describe : str, optional
        Title or description of the chart.

    Returns:
    --------
    str: Path to the generated image.

    Example Usage:
    --------------
    create_pie(
        x_data=["Apple", "Banana", "Orange"],
        y_data=[30, 50, 20],
        x_name="Fruit Type",
        y_name="Sales Quantity",
        describe="Fruit Sales Distribution"
    )
    """
    plt.figure(figsize=(10, 8))

    if isinstance(x_data[0], (list, tuple)):
        x_data = [item[0] for item in x_data]

    if isinstance(y_data[0], (list, tuple)):
        y_data = [item[0] for item in y_data]

    # Check if x_data and y_data are one-dimensional
    if not isinstance(x_data, (list, np.ndarray)) or not isinstance(y_data, (list, np.ndarray)):
        raise TypeError("x_data and y_data must be one-dimensional lists or arrays")

    if len(x_data) != len(y_data):
        raise ValueError("The number of labels and values must be equal")

    total = sum(y_data)
    labels = [f'{label}\n({value}, {value / total * 100:.1f}%)'
              for label, value in zip(x_data, y_data)]

    plt.pie(y_data,
            labels=labels,
            autopct='',
            startangle=90,
            shadow=True)

    plt.axis('equal')

    if describe:
        plt.title(describe)
    else:
        plt.title(f'{x_name} Distribution')

    if x_data:
        plt.legend(x_data,
                   loc='center left',
                   bbox_to_anchor=(1, 0, 0.5, 1))

    plt.tight_layout()
    return _save(plt)


@mcp.tool()
def create_bar(x_data, y_data, x_name="", y_name="", classify=None, describe=""):
    """
    Creates and returns a bar chart showing the distribution of categorical data.

    Parameters:
    -----------
    x_data : list or array-like
        List of category labels for the x-axis.
        Example: ["Apple", "Banana", "Orange"]

    y_data : list or array-like
        List of numerical values corresponding to each category.
        Example: [30, 50, 20]

    x_name : str, optional
        Label for the x-axis indicating the type of categories (e.g., "Month", "Product Type").

    y_name : str, optional
        Label for the y-axis indicating the unit or meaning of the values (e.g., "Sales", "Count").

    classify : list of str, optional
        Classification labels (reserved for future use).

    describe : str, optional
        Title or description of the chart.

    Returns:
    --------
    str: Path to the generated image.

    Example Usage:
    --------------
    create_bar(
        x_data=["Apple", "Banana", "Orange"],
        y_data=[30, 50, 20],
        x_name="Fruit Type",
        y_name="Sales Count",
        describe="Fruit Sales Comparison"
    )
    """
    plt.figure(figsize=(10, 6))

    if isinstance(x_data[0], (list, tuple)):
        x_data = [item[0] for item in x_data]

    if isinstance(y_data[0], (list, tuple)):
        y_data = [item[0] for item in y_data]

    # Check if x_data and y_data are one-dimensional
    if not isinstance(x_data, (list, np.ndarray)) or not isinstance(y_data, (list, np.ndarray)):
        raise TypeError("x_data and y_data must be one-dimensional lists or arrays")

    if len(x_data) != len(y_data):
        raise ValueError("Number of labels and values must match")

    x = np.arange(len(x_data))
    width = 0.35

    bars = plt.bar(x, y_data, width)
    plt.xticks(x, x_data)

    plt.xlabel(x_name)
    plt.ylabel(y_name)

    if describe:
        plt.title(describe)
    else:
        plt.title(f'{x_name} Distribution')

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2., height,
                 f'{int(height)}',
                 ha='center', va='bottom')

    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    return _save(plt)


@mcp.tool()
def create_line(x_data, y_data, x_name="", y_name="", classify=None, describe=""):
    """
    Creates and returns a line chart showing trends over time or another variable.

    Parameters:
    -----------
    x_data : list or array-like
        Data for the x-axis, a one-dimensional list or array of x-values.
        Example: [1, 2, 3, 4, 5]

    y_data : list or array-like
        Data for the y-axis, a one-dimensional list or array of y-values.
        Example: [100, 120, 130, 150, 160]

    x_name : str, optional
        Label for the x-axis, typically the independent variable (e.g., "Time", "Year").

    y_name : str, optional
        Label for the y-axis, typically the dependent variable (e.g., "Temperature", "Revenue").

    classify : list of str, optional
        Labels to differentiate data series. Only one series is supported; this will be used as the legend label.
        Example: ["Company A"]

    describe : str, optional
        Title or description of the chart.

    Returns:
    --------
    str: Path to the generated image.

    Example Usage:
    --------------
    create_line(
        x_data=[1, 2, 3, 4, 5],
        y_data=[100, 120, 130, 150, 160],
        x_name="Month",
        y_name="Sales",
        classify=["Company A"],
        describe="Monthly Sales Trend of Company A"
    )
    """
    plt.figure(figsize=(12, 6))

    if isinstance(x_data[0], (list, tuple)):
        x_data = [item[0] for item in x_data]

    if isinstance(y_data[0], (list, tuple)):
        y_data = [item[0] for item in y_data]

    # Check if x_data and y_data are one-dimensional
    if not isinstance(x_data, (list, np.ndarray)) or not isinstance(y_data, (list, np.ndarray)):
        raise TypeError("x_data and y_data must be one-dimensional lists or arrays")

    if len(x_data) != len(y_data):
        raise ValueError("Number of x and y data points must be equal")

    if classify and len(classify) != 1:
        raise ValueError("Only a single classification label is supported")

    # Draw line plot
    if classify:
        plt.plot(x_data, y_data, marker='o', linestyle='-', label=classify[0])
    else:
        plt.plot(x_data, y_data, marker='o', linestyle='-')

    # Set grid lines
    plt.grid(True, linestyle='--', alpha=0.7)

    # Set axis labels
    plt.xlabel(x_name)
    plt.ylabel(y_name)

    # Set title
    if describe:
        plt.title(describe)

    # Add legend
    if classify:
        plt.legend()

    # Annotate values on data points
    for i, value in enumerate(y_data):
        plt.annotate(str(value),
                     (x_data[i], y_data[i]),
                     textcoords="offset points",
                     xytext=(0, 10),
                     ha='center')

    # Start Y-axis from 0
    plt.gca().set_ylim(bottom=0)

    # Rotate x-axis labels to avoid overlap
    plt.xticks(rotation=45)

    # Adjust layout automatically
    plt.tight_layout()

    return _save(plt)

mcp.run()
