from pathlib import Path

import scrapy
import csv
import os
from urllib.parse import urljoin
import json
from bs4 import BeautifulSoup
class LinkDownloaderSpider(scrapy.Spider):
    name = 'link_downloader'

    def start_requests(self):
        file_folder = Path(__file__).resolve().parents[2]
        file_path = str(file_folder.joinpath("list.csv"))

        if os.path.exists(file_path) and os.path.isfile(file_path):
            print(f"{file_path} exists.")
        else:
            print(f"{file_path} does not exist.")
            raise Exception("File doesn't exist")

        # Read your CSV file
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                url, file_type, depth, comment = row

                # Basic validation (you might add more sophisticated checks)
                if depth.isdigit():
                    depth = int(depth)
                else:
                    depth = 0

                yield scrapy.Request(
                    url,
                    callback=self.parse,
                    meta={'file_type': file_type, 'depth': depth, 'original_url': url}
                )

    def parse_html(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')

        def extract_content(element):
            if element.name == 'p':
                return element.get_text(strip=True)
            else:
                d_children = list(element.children)
                # Recursive call for nested headings
                elements = []
                for child in element.children:
                    if child.name in ['main']:
                        return extract_content(child)

                headers = False
                paragraphs = False
                for child in element.children:
                    if child.name in ['h1', 'h2', 'h3']:
                        headers = True
                    if child.name in ['p']:
                        paragraphs = True

                if paragraphs and not headers:
                    for child in element.children:
                        if child.name in ['p']:
                            child_contents = extract_content(child)
                            elements.append(child_contents)
                if not paragraphs and headers:
                    for child in element.children:
                        if child.name in ['h1', 'h2', 'h3']:
                            name = child.text.strip()
                            contents = extract_content(child)
                            elements[name] = contents
                if paragraphs and headers:
                    for child in element.children:
                        if child.name in ['h1', 'h2', 'h3']:
                            name = child.text.strip()
                            contents = extract_content(child)
                            elements[name] = contents
                    all_para= []
                    for child in element.children:
                        if child.name in ['p']:
                            contents = extract_content(child)
                            all_para.append(contents)
                    elements["paragraph"] = all_para

                if len(elements) == 0:
                    return []

                return {
                    element.name: elements
                }

        data = {
            'url': response.url,
            'content': extract_content(soup.body)  # Start from the <body> tag
        }
        print(data)

        #yield data

    def parse_html2(self, response):
        soup = BeautifulSoup(response.text, 'html.parser')

        def indent(type):
            if type == "h1":
                return " "
            if type == "h2":
                return "    "
            if type == "h3":
                return "        "
            if type == "h4":
                return "        "
            if type == "h5":
                return "        "
            if type == "h6":
                return "        "
            if type == "p":
                return "        "

        def build_nested_dict(items, parent_key='root', result=None):
            if result is None:
                result = {}

            for item in items:
                key = item['content']
                # Check if there are children to process
                if item['children']:
                    # If the key already exists, update it; otherwise, create a new entry
                    if key in result:
                        build_nested_dict(item['children'], parent_key=key, result=result[key])
                    else:
                        result[key] = build_nested_dict(item['children'], parent_key=key)
                else:
                    # If there are no children, append the content to an array under the parent key
                    result.setdefault(parent_key, []).append(key)

            return result

        def extract_content(response):
            try:
                root = []
                current_hierarchy = {0: root}  # Maps header levels to their respective nodes

                for element in response.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p'], recursive=True):
                    content = {'content': element.get_text(strip=True), 'children': []}
                    if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        print(indent(element.name) + element.text.strip())
                        level = int(element.name[1])
                        current_node = current_hierarchy[level - 1]  # Get the parent node
                        current_node.append(content)  # Append the current content to the parent node
                        current_hierarchy[level] = content['children']  # Update the current level's node
                    elif element.name == 'p':
                        current_hierarchy[max(current_hierarchy.keys())].append(
                            content)  # Append paragraph to the nearest header

                nested_dict = build_nested_dict(root)

                return nested_dict
            except Exception as e:
                return []

        for child in soup.body.children:
            print(child.name)
            if child.name in ['main']:
                content = extract_content(child)
                return content

         # Handle paragraphs without a preceding header

    def parse(self, response):


        file_type = response.meta['file_type']
        depth = response.meta['depth']
        current_url = response.url

        print(current_url)

        if file_type == 'PDF':
            # Save the PDF file
            file_folder = Path(__file__).resolve().parents[2].joinpath("downloaded_resources")
            if not file_folder.exists():
                file_folder.mkdir(parents=True)
                print(f"Folder created: {file_folder}")

            filename = response.url.split('/')[-1]
            with open(filename, 'wb') as f:
                f.write(response.body)
            self.log(f'Saved PDF: {filename}')

        elif file_type == 'HTML' and depth > 0:
            #data = self.parse_html(response)
            #print(1)
            self.parse_html2(response)


