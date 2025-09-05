import os
import io
import json
import tempfile
import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import google.generativeai as genai
from gspread import Client as GspreadClient, Spreadsheet

# Ensure these libraries are in your requirements.txt
from faster_whisper import WhisperModel

# --- Configuration (Set these in your GitHub Secrets or environment variables) ---
GCP_SERVICE_ACCOUNT_KEY = os.environ.get("3dc78c5ae3b48b6653150e440deb4907ce289675")
GOOGLE_SHEET_ID = "1xNBNQjT_rAeawAQsGsdXVg3XsgUgCNNUb0v838Chk7s"  # Replace with your actual Sheet ID
PROCESSED_FOLDER_ID = "1OOkneQqGEhgHKU8oIpr8gmUQ7siG1D-5"  # Create a new folder in Drive for processed files
GEMINI_API_KEY = os.environ.get("AIzaSyCf8O_UQdmFEBv_-KFGw3Em8qnPKUxAwsg")

# Configuration for your team folders in Google Drive
TEAM_FOLDERS = {
    {
{
  "Sharath": "https://drive.google.com/drive/folders/1Sb7yKSNIvaXY84OsSIp58M2fi77V0W1m?usp=sharing",
  "Tavish": "https://drive.google.com/drive/folders/1oSUdAVBv0XU73zPgcvKtO5BTt-ph43PI?usp=drive_link",
  "Sripal": "https://drive.google.com/drive/folders/1iI-FGWcQe_8OxpGcn1-IkRZXvfct5D9S?usp=sharing",
  "Musthafa": "https://drive.google.com/drive/folders/1COdFPn7NiNTXzIwMsAC8myVYqKL2fSjq?usp=sharing",
  "Hemanth": "https://drive.google.com/drive/folders/1sxuZ872wPA1a9iDiYK9bQj3SMxk2EA5r?usp=sharing",
  "Luqman": "https://drive.google.com/drive/folders/1l6AEG-41g3oqh5Lldx4-YEljmlwlMqoR?usp=sharing",
  "Darshan": "https://drive.google.com/drive/folders/12oVR5ysssjQDf0vMtG4BXs7qVry7kqkN?usp=sharing",
  "Yash": "https://drive.google.com/drive/folders/1j4pz0pWDqrZzJirE5i6r_RnVOPh1xSRf?usp=sharing",
  "Aditya": "https://drive.google.com/drive/folders/1t1feBlc_6Q3_oMneWHw_NIZ0OZa8D6bY?usp=sharing",
  "Vishal": "https://drive.google.com/drive/folders/1A2I64Drgl4UkV9CNeCYCrprwt7VYeeA-?usp=sharing",
  "Rahul": "https://drive.google.com/drive/folders/1IHGc3a7Fg9tFMxoyZFFZCUQHZzC6Pk_6?usp=drive_link",
  "Aditya_Singh": "https://drive.google.com/drive/folders/1CvYBuI40o6NvpVDgVQnzQIw1xfqAXCra?usp=sharing",
  "Akshay": "https://drive.google.com/drive/folders/1nVlVoBglYS_ib8BWJvQXvpBisyLB_Tfa?usp=sharing",
  "Saleem": "https://drive.google.com/drive/folders/1uEz-fltkZoOkw6-NrZa_mEdg9K-zwuqT?usp=drive_link"
}

}

    # Add all 18 team members and their folder IDs here
}
# --- End of Configuration ---

# Set up basic logging at the beginning of your script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def authenticate_google_services():
    """Authenticates with Google Drive, Sheets, and Gemini APIs."""
    try:
        creds_info = json.loads(GCP_SERVICE_ACCOUNT_KEY)
        creds = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=[
                "https://www.googleapis.com/auth/drive", # 'drive' scope allows moving files
                "https://www.googleapis.com/auth/spreadsheets",
            ],
        )
        
        drive_service = build("drive", "v3", credentials=creds)
        sheets_client = GspreadClient(auth=creds)
        genai.configure(api_key=GEMINI_API_KEY)
        
        logging.info("Successfully authenticated with Google services.")
        return drive_service, sheets_client
    except Exception as e:
        logging.error(f"Authentication failed: {e}")
        return None, None

def download_audio_file(drive_service, file_id, file_name):
    """Downloads an audio file from Google Drive to a temporary location."""
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
        logging.info(f"Download progress: {int(status.progress() * 100)}%")
    
    with tempfile.NamedTemporaryFile(suffix=os.path.splitext(file_name)[1], delete=False) as temp_file:
        temp_file.write(fh.getvalue())
        return temp_file.name

def transcribe_audio(file_path):
    """Transcribes an audio file using Faster-Whisper."""
    try:
        logging.info("Initializing Whisper model...")
        model_size = "tiny.en"  # Can be "base.en" or "small.en" for better accuracy
        model = WhisperModel(model_size, device="cpu", compute_type="int8")
        
        logging.info(f"Transcribing {file_path}...")
        segments, _ = model.transcribe(file_path, beam_size=5)
        
        transcript = ""
        for segment in segments:
            transcript += segment.text
        
        logging.info("Transcription completed successfully.")
        return transcript.strip()
    except Exception as e:
        logging.error(f"Transcription failed: {e}")
        return None

def analyze_transcript_with_gemini(transcript):
    """Analyzes the transcript to extract data points using the Gemini API."""
    try:
        model = genai.GenerativeModel("gemini-1.5-pro")
        prompt = f"""
        Analyze the following meeting transcript and extract the following 47 data points.
        Please format the output as a JSON object with the specified keys.
        If a data point cannot be found, use "N/A" as the value.

        Transcript:
        {transcript}

        Data points to extract:
        1. Date
        2. POC Name
        3. Society Name
        4. Visit Type
        5. Meeting Type
        6. Amount Value
        7. Months
        8. Deal Status
        9. Vendor Leads
        10. Society Leads
        11. Opening Pitch Score
        12. Product Pitch Score
        13. Cross-Sell / Opportunity Handling
        14. Closing Effectiveness
        15. Negotiation Strength
        16. Overall Sentiment
        17. Total Score
        18. % Score
        19. Risks / Unresolved Issues
        20. Improvements Needed
        21. Owner (Who handled the meeting)
        22. Email Id
        23. Kibana ID
        24. Manager
        25. Product Pitch
        26. Team
        27. Media Link
        28. Doc Link
        29. Suggestions & Missed Topics
        30. Pre-meeting brief
        31. Meeting duration (min)
        32. Rebuttal Handling
        33. Rapport Building
        34. Improvement Areas
        35. Product Knowledge Displayed
        36. Call Effectiveness and Control
        37. Next Step Clarity and Commitment
        38. Missed Opportunities
        39. Key Discussion Points
        40. Key Questions
        41. Competition Discussion
        42. Action items
        43. Positive Factors
        44. Negative Factors
        45. Customer Needs
        46. Overall Client Sentiment
        47. Feature Checklist Coverage

        Output format (JSON):
        {{
          "Date": "...",
          "POC Name": "...",
          "Society Name": "...",
          "Visit Type": "...",
          "Meeting Type": "...",
          "Amount Value": "...",
          "Months": "...",
          "Deal Status": "...",
          "Vendor Leads": "...",
          "Society Leads": "...",
          "Opening Pitch Score": "...",
          "Product Pitch Score": "...",
          "Cross-Sell / Opportunity Handling": "...",
          "Closing Effectiveness": "...",
          "Negotiation Strength": "...",
          "Overall Sentiment": "...",
          "Total Score": "...",
          "Percent Score": "...",
          "Risks / Unresolved Issues": "...",
          "Improvements Needed": "...",
          "Owner": "...",
          "Email Id": "...",
          "Kibana ID": "...",
          "Manager": "...",
          "Product Pitch": "...",
          "Team": "...",
          "Media Link": "...",
          "Doc Link": "...",
          "Suggestions & Missed Topics": "...",
          "Pre-meeting brief": "...",
          "Meeting duration (min)": "...",
          "Rebuttal Handling": "...",
          "Rapport Building": "...",
          "Improvement Areas": "...",
          "Product Knowledge Displayed": "...",
          "Call Effectiveness and Control": "...",
          "Next Step Clarity and Commitment": "...",
          "Missed Opportunities": "...",
          "Key Discussion Points": "...",
          "Key Questions": "...",
          "Competition Discussion": "...",
          "Action items": "...",
          "Positive Factors": "...",
          "Negative Factors": "...",
          "Customer Needs": "...",
          "Overall Client Sentiment": "...",
          "Feature Checklist Coverage": "..."
        }}
        """
        response = model.generate_content(
            prompt,
            generation_config={"response_mime_type": "application/json"}
        )
        
        logging.info("Successfully analyzed transcript with Gemini.")
        return json.loads(response.text)
    except Exception as e:
        logging.error(f"Gemini API analysis failed: {e}")
        return None

def write_to_google_sheets(sheets_client, spreadsheet_id, data):
    """Appends a new row of data to the Google Sheet."""
    try:
        sheet = sheets_client.open_by_key(spreadsheet_id)
        worksheet = sheet.get_worksheet(0) # Assumes the first sheet
        
        # Ensure the order of data matches your sheet's columns
        row_values = [
            data.get("Date", "N/A"),
            data.get("POC Name", "N/A"),
            data.get("Society Name", "N/A"),
            data.get("Visit Type", "N/A"),
            data.get("Meeting Type", "N/A"),
            data.get("Amount Value", "N/A"),
            data.get("Months", "N/A"),
            data.get("Deal Status", "N/A"),
            data.get("Vendor Leads", "N/A"),
            data.get("Society Leads", "N/A"),
            data.get("Opening Pitch Score", "N/A"),
            data.get("Product Pitch Score", "N/A"),
            data.get("Cross-Sell / Opportunity Handling", "N/A"),
            data.get("Closing Effectiveness", "N/A"),
            data.get("Negotiation Strength", "N/A"),
            data.get("Overall Sentiment", "N/A"),
            data.get("Total Score", "N/A"),
            data.get("Percent Score", "N/A"),
            data.get("Risks / Unresolved Issues", "N/A"),
            data.get("Improvements Needed", "N/A"),
            data.get("Owner", "N/A"),
            data.get("Email Id", "N/A"),
            data.get("Kibana ID", "N/A"),
            data.get("Manager", "N/A"),
            data.get("Product Pitch", "N/A"),
            data.get("Team", "N/A"),
            data.get("Media Link", "N/A"),
            data.get("Doc Link", "N/A"),
            data.get("Suggestions & Missed Topics", "N/A"),
            data.get("Pre-meeting brief", "N/A"),
            data.get("Meeting duration (min)", "N/A"),
            data.get("Rebuttal Handling", "N/A"),
            data.get("Rapport Building", "N/A"),
            data.get("Improvement Areas", "N/A"),
            data.get("Product Knowledge Displayed", "N/A"),
            data.get("Call Effectiveness and Control", "N/A"),
            data.get("Next Step Clarity and Commitment", "N/A"),
            data.get("Missed Opportunities", "N/A"),
            data.get("Key Discussion Points", "N/A"),
            data.get("Key Questions", "N/A"),
            data.get("Competition Discussion", "N/A"),
            data.get("Action items", "N/A"),
            data.get("Positive Factors", "N/A"),
            data.get("Negative Factors", "N/A"),
            data.get("Customer Needs", "N/A"),
            data.get("Overall Client Sentiment", "N/A"),
            data.get("Feature Checklist Coverage", "N/A"),
        ]
        
        worksheet.append_row(row_values, value_input_option="USER_ENTERED")
        logging.info("Data successfully written to Google Sheets.")
    except Exception as e:
        logging.error(f"Failed to write to Google Sheets: {e}")

def move_file_to_processed(drive_service, file_id, source_folder_id, processed_folder_id):
    """Moves a file from its source folder to a 'Processed' folder."""
    try:
        drive_service.files().update(
            fileId=file_id,
            addParents=processed_folder_id,
            removeParents=source_folder_id,
            fields="id, parents"
        ).execute()
        logging.info(f"Successfully moved file {file_id} to 'Processed' folder.")
    except Exception as e:
        logging.error(f"Failed to move file {file_id}: {e}")

def main():
    drive_service, sheets_client = authenticate_google_services()
    if not drive_service or not sheets_client:
        return

    for member_name, folder_id in TEAM_FOLDERS.items():
        logging.info(f"Checking folder for team member: {member_name}")
        try:
            results = drive_service.files().list(
                q=f"'{folder_id}' in parents and (mimeType='audio/mpeg' or mimeType='video/mp4' or mimeType='audio/mp4')",
                fields="nextPageToken, files(id, name)",
            ).execute()
            files = results.get("files", [])
            
            if not files:
                logging.info(f"No new audio files found for {member_name}.")
                continue
            
            for file in files:
                file_id = file["id"]
                file_name = file["name"]
                logging.info(f"Processing file: {file_name}")
                
                temp_file_path = download_audio_file(drive_service, file_id, file_name)
                
                transcript = transcribe_audio(temp_file_path)
                if transcript:
                    analysis_data = analyze_transcript_with_gemini(transcript)
                    
                    if analysis_data:
                        write_to_google_sheets(sheets_client, GOOGLE_SHEET_ID, analysis_data)
                        move_file_to_processed(drive_service, file_id, folder_id, PROCESSED_FOLDER_ID)
                
                os.remove(temp_file_path)
                logging.info(f"Cleaned up temporary file: {temp_file_path}")

        except Exception as e:
            logging.error(f"An error occurred while processing {member_name}'s folder: {e}")

if __name__ == "__main__":
    main()
