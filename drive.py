from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import os

def authenticate_drive():
    """Authenticates the user and returns a GoogleDrive object."""
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()  # Creates local webserver and auto handles authentication
    return GoogleDrive(gauth)

def get_file_id_by_name(drive, folder_id, file_name):
    """
    Finds the file ID of a file with a given name in a specified Google Drive folder.

    Args:
        drive: GoogleDrive object authenticated with PyDrive.
        folder_id: The ID of the folder to search in.
        file_name: The name of the file to search for.

    Returns:
        The file ID if found, or None if no matching file is found.
    """
    query = f"'{folder_id}' in parents and title = '{file_name}' and mimeType = 'text/csv'"
    file_list = drive.ListFile({'q': query}).GetList()
    if file_list:
        return file_list[0]['id']  # Return the first match (assuming unique file names)
    return None

def overwrite_csv_on_drive(drive, file_id, updated_df, temp_file_name="csv files/combined_order_flow.csv"):
    """
    Overwrites an existing CSV file on Google Drive with updated content.

    Args:
        drive: GoogleDrive object authenticated with PyDrive.
        file_id: The ID of the file to overwrite.
        updated_df: Pandas DataFrame containing updated content.
        temp_file_name: Temporary file name for saving updated CSV locally.
    """
    # Save the updated DataFrame to a temporary file
    updated_df.to_csv(temp_file_name, index=False)
    
    # Overwrite the file on Google Drive
    file_drive = drive.CreateFile({'id': file_id})
    file_drive.SetContentFile(temp_file_name)
    file_drive.Upload()
    
    # Clean up the temporary file
    # os.remove(temp_file_name)
    print(f"File with ID {file_id} has been successfully overwritten.")

def list_csv_files(drive, folder_id):
    """Lists all CSV files in a specified folder."""
    query = f"'{folder_id}' in parents and mimeType = 'text/csv'"
    file_list = drive.ListFile({'q': query}).GetList()
    return file_list

def read_csv_from_drive(drive, file_id):
    """Downloads a CSV file from Google Drive and reads it into a pandas DataFrame."""
    file_drive = drive.CreateFile({'id': file_id})
    file_drive.GetContentFile("temp.csv")  # Downloads the file as 'temp.csv'
    df = pd.read_csv("temp.csv")
    os.remove("temp.csv")  # Clean up the temporary file
    return df

def upload_csv_to_drive(drive, folder_id, csv_file_path):
    """Uploads a CSV file to the specified Google Drive folder."""
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"The file '{csv_file_path}' does not exist.")
    file_name = os.path.basename(csv_file_path)
    file_metadata = {
        "title": file_name,
        "parents": [{"id": folder_id}],
        "mimeType": "text/csv"
    }
    file_drive = drive.CreateFile(file_metadata)
    file_drive.SetContentFile(csv_file_path)
    file_drive.Upload()
    print(f"Uploaded file '{file_name}' to folder ID: {folder_id}")

# Example usage
if __name__ == "__main__":
    folder_id = '1zbZyKzAwn3yKSCWI4VwxRhmdI439kLyp'  # Replace with your folder's ID
    drive = authenticate_drive()
    
    # List all CSV files in the folder
    # csv_files = list_csv_files(drive, folder_id)
    # if not csv_files:
    #     print("No CSV files found in the folder.")
    # else:
    #     for file in csv_files:
    #         print(f"Found CSV: {file['title']} (ID: {file['id']})")
        
    #     selected_file_id = input("\nEnter the ID of the CSV file you want to download: ").strip()
        
    #     # Check if the entered ID matches any file in the folder
    #     matching_files = [file for file in csv_files if file['id'] == selected_file_id]
    #     if not matching_files:
    #         print("Invalid file ID. Please ensure you entered the correct ID from the list.")
    #     else:
    #         # Download and read the selected CSV file into a DataFrame
    #         df = read_csv_from_drive(drive, selected_file_id)
    #         print("\nContent of the selected CSV file:")
    #         print(df)
    
    # Upload a CSV file to the Google Drive folder
    try:
        upload_csv_to_drive(drive, folder_id, 'RP Order Flow - rp_order_flow.csv')
    except FileNotFoundError as e:
        print(e)