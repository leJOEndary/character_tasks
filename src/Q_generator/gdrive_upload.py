from tqdm import tqdm
from src.gdrive_api.folder_upload import upload_folder
from src.gdrive_api.auth import build_service

def build_service_and_upload_files(creds_path, folder_path, destination_url):
    """
    Builds the Google Drive service and uploads files from a specified folder to a Google Drive folder.

    Parameters:
    - creds_path (str): Path to the credentials file for Google Drive service authentication.
    - folder_path (str): Local path of the folder whose contents are to be uploaded.
    - destination_url (str): URL of the Google Drive folder where files will be uploaded.

    Returns:
    - dict: A dictionary mapping uploaded file paths to their new Google Colab URLs.
    """
    service = build_service(creds_path)
    uploaded_files = upload_folder(service, folder_path, destination_url, force_replace=True, is_url=True)

    file_path_to_url = {}
    with tqdm(total=len(uploaded_files)) as pbar:
        for file_path, file_url in uploaded_files.items():
            if file_url is not None:
                drive_id = file_url.split("id=")[-1].split("&")[0].strip()
                colab_url = f"https://colab.research.google.com/drive/{drive_id}"
                file_path_to_url[file_path] = colab_url
            else:
                print(f"Skipped uploading {file_path}")
            pbar.update(1)

    return file_path_to_url

def update_problems_with_metadata(file_path_to_url, file_path_to_problem, batch_idx):
    """
    Updates the metadata of problems with their respective Google Colab URLs and other information.

    Parameters:
    - file_path_to_url (dict): A dictionary mapping file paths to their corresponding Google Colab URLs.
    - file_path_to_problem (dict): A dictionary mapping file paths to problem data.
    - batch_idx (str): The index of the batch being processed.

    Modifies:
    - The 'file_path_to_problem' dictionary is updated with additional metadata for each problem.
    """
    for file_path in file_path_to_url.keys():
        if file_path == "problems.json":
            continue
        problem = file_path_to_problem[file_path]
        problem["metadata"]["colab_url"] = file_path_to_url[file_path]
        problem["metadata"]["file_path"] = file_path
        problem["metadata"]["batch_idx"] = batch_idx