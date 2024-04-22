from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import requests
from PIL import Image
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Google Sheets authentication and access
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name('', scope) ## json keyfile
client = gspread.authorize(creds)
# Replace 'your_sheet_id_here' with your Google Sheet ID
SHEET_ID = '1MCEvcOdr47OuHlFHEgYi9ZdeWLc145pdAxGEzfkLHhM'
sheet = client.open_by_key(SHEET_ID).sheet1

# Access the links from Google Sheet
links = sheet.col_values(1)[1:]  # Assuming the first row contains headers

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Cache-Control": "max-age=0",
}

def get_image_resolution(image_url):
    """
    Updates a specific cell in a Google Sheet with the given image resolution.

    Args:
        row_index (int): The index of the row to update.
        image_resolution (tuple): A tuple containing the width and height of the image.

    Returns:
        str: An error message if an exception occurs during the update, otherwise None.
    """
    try:
        response = requests.get(image_url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            image = Image.open(BytesIO(response.content))
            image_resolution = image.size
            return image_resolution
        return f"Failed to fetch image (HTTP status code {response.status_code})"
    except Exception as e:
        return f"Error: {e}"

def update_google_sheet(row_index, image_resolution):
    """
    Updates a specific cell in a Google Sheet with the given image resolution.

    Args:
        row_index (int): The index of the row to update.
        image_resolution (tuple): A tuple containing the width and height of the image.

    Returns:
        str: An error message if an exception occurs during the update, otherwise None.
    """
    try:
        sheet.update_cell(row_index + 2, 2, f"{image_resolution[0]}x{image_resolution[1]}")
    except Exception as e:
        return f"Error: {str(e)}"

if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=50) as executor:
        resolutions = list(executor.map(get_image_resolution, links))

    for i, resolution in enumerate(resolutions):
        update_google_sheet(i, resolution)

    print("Resolutions updated in Google Sheet.")
