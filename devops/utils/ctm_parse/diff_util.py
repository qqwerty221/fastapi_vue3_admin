import xml.etree.ElementTree as ET
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO, format='%(message)s')

def compare_elements(element1, element2, path=""):
    path += f"/{element1.tag}" if path else element1.tag

    # Compare tag names
    if element1.tag != element2.tag:
        logging.info(f"标签不一致: {path}")
        return False

    # Compare attributes
    if element1.attrib != element2.attrib:
        logging.info(f"属性不一致: {path}")
        return False

    # Compare text
    if element1.text != element2.text:
        logging.info(f"文本不一致: {path}")
        return False

    # Sort and compare child elements by tag name
    children1 = sorted(element1, key=lambda e: e.tag)
    children2 = sorted(element2, key=lambda e: e.tag)

    if len(children1) != len(children2):
        logging.info(f"子元素数量不一致: {path}")
        return False

    for child1, child2 in zip(children1, children2):
        if not compare_elements(child1, child2, path):
            return False

    return True

def compare_xml_files(file1, file2):
    tree1 = ET.parse(file1)
    tree2 = ET.parse(file2)

    root1 = tree1.getroot()
    root2 = tree2.getroot()

    return compare_elements(root1, root2)

# Example usage
file1 = '../test/test1.xml'
file2 = '../test/test2.xml'

if compare_xml_files(file1, file2):
    print("XML 文件一致")
else:
    print("XML 文件不一致")
