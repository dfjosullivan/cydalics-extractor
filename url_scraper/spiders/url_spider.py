import logging
import re
import shutil
from pathlib import Path

import scrapy
import csv
import os
from urllib.parse import urlparse
import json
from bs4 import BeautifulSoup
class LinkDownloaderSpider(scrapy.Spider):
    name = 'link_downloader'
    allowed_domains = []
    logger = logging.getLogger()

    def extract_domains(self, urls):
        domains = set()
        for url in urls:
            try:
                parsed_url = urlparse(url)
                domains.add(parsed_url.netloc)
            except Exception as e:
                self.logger.exception(e)
        return domains

    def start_requests(self):
        file_folder = Path(__file__).resolve().parents[2]
        file_path = str(file_folder.joinpath("list.csv"))

        if os.path.exists(file_path) and os.path.isfile(file_path):
            print(f"{file_path} exists.")
        else:
            print(f"{file_path} does not exist.")
            raise Exception("File doesn't exist")

        directory_path = Path(__file__).resolve().parents[2].joinpath("downloaded_resources")
        # Check if the directory exists
        if os.path.exists(directory_path) and os.path.isdir(directory_path):
            # Remove the directory and all its contents
            shutil.rmtree(directory_path)
            print(f"Directory {directory_path} has been removed.")
        else:
            print(f"Directory {directory_path} does not exist.")

        urls = []
        # Read your CSV file
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                url, file_type, depth, comment = row
                if file_type == "HTML":
                    urls.append(url)
        self.allowed_domains = self.extract_domains(urls)

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
                self.logger.exception(e)
                return []

        if soup.body is None or soup.body.children is None:
            self.logger.info("No children found")
            return {}


        for child in soup.body.children:
            if child.name in ['main']:
                content = extract_content(child)
                return content

         # Handle paragraphs without a preceding header

    def url_to_filename(self, url):
        try:
            # Remove the protocol (http or https) and replace it with an empty string
            filename = re.sub(r'^https?://', '', url)

            # Replace invalid filename characters with underscores
            filename = re.sub(r'[^a-zA-Z0-9\-_\.]', '_', filename)

            # Optional: Trim the length of the filename
            max_length = 255  # Maximum filename length for most file systems
            if len(filename) > max_length:
                extension = filename.rsplit('.', 1)[1]
                filename = filename[:max_length - len(extension) - 1] + '.' + extension

            return filename
        except Exception as e:
            self.logger.exception(e)

    def is_valid_url(self, url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except ValueError:
            return False

    def get_file_folder(self):
        try:
            file_folder = Path(__file__).resolve().parents[2].joinpath("downloaded_resources")

            if not file_folder.exists():
                file_folder.mkdir(parents=True)
                self.logger.info(f"Folder created: {file_folder}")

            return file_folder
        except Exception as e:
            self.logger.exception(e)
            raise Exception("Failed to get folder")

    def update_downloaded_csv(self, current_url, file_type):
        try:
            file_folder = self.get_file_folder()
            filename = file_folder.joinpath("downloaded.csv")
            new_row = [current_url, file_type]

            with open(filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(new_row)
        except Exception as e:
            self.logger.exception(e)

    def save_pdf(self,
                 response):
        try:
            file_folder = self.get_file_folder()
            filename = response.url.split('/')[-1]
            filename = file_folder.joinpath(filename)
            with open(str(filename), 'wb') as f:
                f.write(response.body)
            self.logger.info(f'Saved PDF: {filename}')
        except Exception as e:
            self.logger.exception(e)

    def update_url_skipped(self,
                           current_url,
                           context_url,
                           file_type,
                           reason):
        try:
            file_folder = self.get_file_folder()
            filename = file_folder.joinpath("downloaded.csv")
            new_row = [current_url, context_url, file_type, reason]

            with open(filename, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(new_row)
        except Exception as e:
            self.logger.exception(e)

    def save_html(self,
                  response,
                  current_url,
                  file_type):
        try:
            if response is None or response.text is None:
                self.update_url_skipped(current_url, file_type, current_url, "Response text is empty ")
                self.logger.info("Skipping " + current_url + " not valid ")
                return

            content = {}
            content["content"] = self.parse_html2(response)

            content["file_type"] = file_type
            content["url"] = current_url
            # Save the PDF file
            file_folder = self.get_file_folder()

            filename = file_folder.joinpath(self.url_to_filename(current_url))
            # Writing JSON data
            with open(str(filename) + ".json", 'w') as f:
                json.dump(content, f)
            self.logger.info("Downloaded page " + str(filename))
        except Exception as e:
            self.logger.exception(e)

    def parse(self, response):


        file_type = response.meta['file_type']
        depth = response.meta['depth']
        current_url = response.url

        self.logger.info("Processing " + current_url)

        self.update_downloaded_csv(current_url, file_type)

        if file_type == 'PDF':
            # Save the PDF file
            self.save_pdf(response)

        elif file_type == 'HTML':
            self.save_html(response, current_url, file_type)

            # Follow links within the same domain only, respecting the depth limit
            soup = BeautifulSoup(response.text, 'html.parser')
            for a in soup.find_all('a', href=True):
                link = a['href']
                # Check if the link is within the same domain
                if not self.is_valid_url(link):
                    self.update_url_skipped(link, file_type, current_url, "Response text is empty ")
                    self.logger.info("Skipping " + link + " not valid ")
                    continue

                if link.lower().endswith('.pdf') or "pdf" in link.lower():
                    new_file_type = "PDF"
                else:
                    new_file_type = "HTML"

                if urlparse(link).netloc == '' or urlparse(link).netloc in self.allowed_domains:
                    self.logger.info("Added " + link)
                    yield response.follow(link, self.parse, meta={'file_type': new_file_type, 'depth': 2, 'original_url': link})
                elif urlparse(link).netloc not in self.allowed_domains:
                    self.logger.info("Did not add (domain) " + link)
                    self.update_url_skipped(link, file_type, current_url, "Outside allowed domains")
                elif urlparse(link).netloc:
                    self.logger.info("Did not add (url) " + link)
                    self.update_url_skipped(link, file_type, current_url, "Not url")



