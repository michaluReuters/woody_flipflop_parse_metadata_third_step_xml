import xml.etree.ElementTree as ET


def extract_value_from_xml(xml_data, tag_path):
    """
    Extracts the value of an element from XML data using the specified tag path.

    Args:
    xml_data (str): A string containing the XML data.
    tag_path (str): The path of the tag of the element whose value is to be extracted.

    Returns:
    str: The value of the element.

    """
    root = ET.fromstring(xml_data)
    element = root.find(f"./{tag_path.replace('.', '/')}")
    if element is not None:
        return element.text
    else:
        pass
