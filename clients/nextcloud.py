# import packages
import re
import requests
from os import getenv
from io import BytesIO
import xml.etree.ElementTree as ET
from urllib.parse import quote, unquote 

from utils.logger_config import get_logger
logger = get_logger()

class NEXTCLOUD():
    def __init__(
            self,
            conn_id: str | None = None,
            is_local_client: bool | None = None
    ):
        self.conn_id = conn_id or "nextcloud_default"
        self.is_local_client = is_local_client

        self.is_local_client = self._detect_local(self.is_local_client)
        self._config = self._get_config()
        

    def _detect_local(self, is_local_client: bool) -> bool:
        """True - локальный запуск, False - Airflow, None - определяем автоматически."""
        if is_local_client is not None:
            return is_local_client
        is_airflow_home = getenv("AIRFLOW_HOME") or getenv("AIRFLOW__CORE__DAGS_FOLDER")
        return not is_airflow_home

    def _get_config(self) -> dict[str, str]:
        if self.is_local_client:
            return self._load_local_config()
        return self._load_airflow_config()

    def _load_airflow_config(self) -> dict[str, str]:
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection(self.conn_id)

        return {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password
        }
    
    def _load_local_config(self) -> dict[str, str]:
        from dotenv import load_dotenv
        load_dotenv()

        return {
            "host": getenv("NEXTCLOUD_HOST"),
            "user": getenv("NEXTCLOUD_LOGIN_USER"),
            "password": getenv("NEXTCLOUD_APP_PASSWORD")
        }

    @staticmethod
    def _encode_path(path: str) -> str:
        """URL-encode each path segment separately.

        Keeps forward slashes intact so the full path structure is
        preserved.

        Args:
            path: Raw path string, e.g. "folder/file name.txt".

        Returns:
            Path with individual segments percent-encoded.
        """
        return "/".join(quote(part) for part in path.split("/"))

    def download_file(self, path: str) -> BytesIO:
        """Download a file from Nextcloud into memory.

        Args:
            path: Full str-path to the file in Nextcloud.

        Returns:
            A BytesIO buffer containing the file content.
        """
        config = self._config
        host = config['host']
        user = config['user']
        password = config['password']
        encode_path = self._encode_path(path)

        web_dav_url = f"{host}/remote.php/dav/files/{quote(user)}/{encode_path}"
        headers = {'Content-Type': 'application/xml'}

        response = requests.get(
            url=web_dav_url, 
            auth=(user, password),
            headers=headers,
            timeout=60)
        response.raise_for_status()

        buffer = BytesIO(response.content)

        logger.info(f"[OK] File downloaded: {path}")
        return buffer

    def upload_file(self, buffer: BytesIO, path: str, f_name: str = ""):
        """Upload a file to Nextcloud via WebDAV PUT.

        If *f_name* is provided it is appended to *path*. Make sure
        *path* ends with / when uploading to a folder, otherwise
        the file name will be concatenated directly.

        Args:
            buffer: In-memory file object to upload.
            path: Target directory or full target path in Nextcloud.
            f_name: Optional file name to append to *path*.

        Raises:
            requests.HTTPError: On non-2xx responses from the server.
        """
        config = self._config
        host = config['host']
        user = config['user']
        password = config['password']
        if f_name: path += f_name
        encode_path = self._encode_path(path)

        web_dav_url = f"{host}/remote.php/dav/files/{quote(user)}/{encode_path}"
        headers = {"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}

        response = requests.put(
            url=web_dav_url,
            auth=(user, password),
            headers=headers,
            data=buffer,
            timeout=300,
        )
        response.raise_for_status()
        logger.info(f"[OK] File uploaded: {path}")

    def read_folder_files(self, path: str, re_filter: str = None) -> dict[str, str]:
        """List files inside a Nextcloud directory via WebDAV PROPFIND.

        Skips sub-folders (collections) and returns only regular files.
        If *re_filter* is supplied only file names matching the pattern
        are included.

        Args:
            path: Directory path in Nextcloud.
            re_filter: Optional regular expression pattern to filter
                file names, e.g. r"\\.xlsx$".

        Returns:
            Mapping {file_name: full_path} for every matched file.
        """
        config = self._config
        host = config['host']
        user = config['user']
        password = config['password']
        encode_path = self._encode_path(path)

        web_dav_url = f"{host}/remote.php/dav/files/{quote(user)}/{encode_path}"
        headers = {'Content-Type': 'application/xml'}

        propfind_body = """<?xml version="1.0"?>
        <d:propfind xmlns:d="DAV:">
            <d:prop>
                <d:displayname/>
                <d:resourcetype/>
            </d:prop>
        </d:propfind>"""
        
        response = requests.request(
            'PROPFIND',
            url=web_dav_url,
            auth=(user, password),
            headers=headers,
            data=propfind_body,
            timeout=60
        )
        response.raise_for_status()
        
        # Parse WebDAV PROPFIND XML response
        root = ET.fromstring(response.content)
        ns = {'d': 'DAV:'}
        
        files = dict()
        
        for resp in root.findall('.//d:response', ns):
            href_elem = resp.find('d:href', ns)
            resourcetype_elem = resp.find('.//d:collection', ns)
            
            if href_elem is None or resourcetype_elem is not None:
                continue
            
            # get file name 
            href = href_elem.text
            filename_encoded = href.strip('/').split('/')[-1]
            
            filename = unquote(filename_encoded)
            
            # re-filter file names
            if re_filter is None or re.search(re_filter, filename):
                files[filename] = f"{path}/{filename}"
        
        logger.info(f"[OK] Folder reader, cnt files: {len(files)}")
        return files

    def check_existence_object(self, path: str) -> bool:
        """Check whether a object existence on Nextcloud.

        Uses a lightweight WebDAV HEAD request.

        Args:
            path: Full path to the object.

        Returns:
            True if the server responds with HTTP 200,
            False otherwise.
        """
        config = self._config
        host = config['host']
        user = config['user']
        password = config['password']
        encode_path = self._encode_path(path)
        web_dav_url = f"{host}/remote.php/dav/files/{quote(user)}/{encode_path}"

        response = requests.head(
            url=web_dav_url,
            auth=(user, password),
            timeout=30
        )
        return response.status_code == 200
