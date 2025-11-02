import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from urllib.parse import urljoin
import re
import os

# --- Core Scraping Functions ---
# [ ... The full, unchanged scrape_school function and its helpers go here ... ]

def navigate_to_master_table(session: requests.Session, day_of_month: int) -> BeautifulSoup:
    """Handles the complex navigation to the master school attendance list."""
    home_url = "https://edudel.nic.in/mis/eis/Attendance/frmAttendanceFirstPageHome.aspx"
    res_home = session.get(home_url, timeout=45)
    soup_home = BeautifulSoup(res_home.content, 'lxml')
    vs, ev = (soup_home.find('input', {'name': n}).get('value') for n in ['__VIEWSTATE', '__EVENTVALIDATION'])
    day_payload = {'__VIEWSTATE': vs, '__EVENTVALIDATION': ev, 'ddlDate1': str(day_of_month), 'btnNext': 'Next'}
    res_menu = session.post(home_url, data=day_payload, timeout=45)
    soup_menu = BeautifulSoup(res_menu.content, 'lxml')
    vs_menu, ev_menu = (soup_menu.find('input', {'name': n}).get('value') for n in ['__VIEWSTATE', '__EVENTVALIDATION'])
    postback_payload = {'__VIEWSTATE': vs_menu, '__EVENTVALIDATION': ev_menu, '__EVENTTARGET': 'LnkBtnAllSchool'}
    res_all_schools_page = session.post(home_url, data=postback_payload, timeout=45)
    soup_all_schools = BeautifulSoup(res_all_schools_page.content, 'lxml')
    vs_all, ev_all = (soup_all_schools.find('input', {'name': n}).get('value') for n in ['__VIEWSTATE', '__EVENTVALIDATION'])
    summary_payload = {'__VIEWSTATE': vs_all, '__EVENTVALIDATION': ev_all, '__EVENTTARGET': 'LinkDescription1'}
    res_master_table_page = session.post(home_url, data=summary_payload, timeout=45)
    if res_master_table_page.status_code == 200:
        return BeautifulSoup(res_master_table_page.content, 'lxml')
    return None

def extract_school_tasks(soup_master: BeautifulSoup, school_id: str) -> tuple:
    """Parses the master table to find the target school and extract scraping tasks."""
    master_table = soup_master.find('table', {'class': 'mistable'})
    if not master_table: return [], None, None
    all_rows = master_table.find_all('tr')
    header_row = all_rows[1]
    headers = [cell.text.strip() for cell in header_row.find_all('td')]
    target_row = next((row for row in all_rows[2:] if school_id in row.text), None)
    if not target_row: return [], None, None
    row_cells = target_row.find_all('td')
    stored_school_id = row_cells[1].text.strip()
    stored_school_name = row_cells[2].text.strip()
    tasks = []
    home_url = "https://edudel.nic.in/mis/eis/Attendance/frmAttendanceFirstPageHome.aspx"
    for i, cell in enumerate(row_cells):
        link_tag = cell.find('a')
        if link_tag and link_tag.text.strip().isdigit() and int(link_tag.text.strip()) > 0:
            status = headers[i]
            detail_url = urljoin(home_url, link_tag['href'])
            tasks.append({'status': status, 'url': detail_url})
    return tasks, stored_school_id, stored_school_name

def scrape_detail_pages(session: requests.Session, tasks: list, school_id: str, school_name: str) -> list:
    """Visits each detail URL and scrapes the final employee data."""
    all_teacher_data = []
    for item in tasks:
        print(f"  -> Scraping '{item['status']}'...")
        res_detail = session.get(item['url'], timeout=45)
        soup_detail = BeautifulSoup(res_detail.content, 'lxml')
        emp_table = soup_detail.find('table', {'class': 'mistable'})
        if emp_table:
            for emp_row in emp_table.find_all('tr'):
                row_text = emp_row.text
                if 'Employee ID' in row_text and 'Employee Name' in row_text:
                    continue
                cols = [cell.text.strip() for cell in emp_row.find_all('td')]
                if len(cols) >= 4:
                    all_teacher_data.append({"school_id": school_id, "school_name": school_name, "employee_id": cols[1], "employee_name": cols[2], "post": cols[3], "attendance_status": item['status']})
    return all_teacher_data

def scrape_school(school_id: str, day_of_month: int) -> pd.DataFrame:
    """Orchestrates the scraping process for a single school on a single day."""
    try:
        with requests.Session() as s:
            s.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'})
            soup_master = navigate_to_master_table(s, day_of_month)
            if not soup_master: return pd.DataFrame()
            tasks, stored_id, stored_name = extract_school_tasks(soup_master, school_id)
            if not tasks: return pd.DataFrame()
            scraped_data = scrape_detail_pages(s, tasks, stored_id, stored_name)
            return pd.DataFrame(scraped_data)
    except requests.exceptions.RequestException as e: print(f" -> Network error for school {school_id}: {e}")
    except Exception as e: print(f" -> An unexpected error occurred for school {school_id}: {e}")
    return pd.DataFrame()

# --- Main Execution Logic ---
if __name__ == "__main__":
    SCHOOL_IDS_TO_SCRAPE = ["1002001", "1001001"] #List of schools ID's for which data to be scraped
    DAYS_TO_SCRAPE = [1] #List of days of the current month for which data to be scraped

    print(f"--- Starting Scraper for {len(SCHOOL_IDS_TO_SCRAPE)} School(s) across {len(DAYS_TO_SCRAPE)} Day(s) ---")
    all_data = []

    for day in DAYS_TO_SCRAPE:
        print(f"\n--- Processing Day: {day} of the current month ---")
        for school_id in SCHOOL_IDS_TO_SCRAPE:
            print(f"Scraping data for School ID: {school_id}...")
            school_day_df = scrape_school(school_id=school_id, day_of_month=day)
            
            if not school_day_df.empty:
                school_day_df['date'] = f"{day}-{datetime.now().month}-{datetime.now().year}"
                print(f" -> Success! Found {len(school_day_df)} records.")
                all_data.append(school_day_df)
            else:
                print(f" -> Finished. No data found for this school on this day.")

    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        print("\n\n--- SCRAPING PROCESS COMPLETE ---")
        print("First 5 records:")
        print(combined_df.head())
        print("\nLast 5 records:")
        print(combined_df.tail())
        print(f"\nTotal records scraped from all schools and days: {len(combined_df)}")
        
        # --- YOUR RESTORED AND IMPROVED LOGIC ---
        try:
            # Define the output folder and filename
            output_folder = "data"
            os.makedirs(output_folder, exist_ok=True)
            school_ids_str = "_".join(SCHOOL_IDS_TO_SCRAPE)
            days_str = "_".join([f"{d:02d}" for d in DAYS_TO_SCRAPE])
            date_stamp = datetime.now().strftime('%Y%m')
            filename = f"attendance_{school_ids_str}_days_{days_str}_{date_stamp}.csv"
            full_path = os.path.join(output_folder, filename)

            # Attempt to save the file
            combined_df.to_csv(full_path, index=False)
            print(f"\nAll data successfully saved to '{full_path}'")
            
        except OSError as e:
            # If saving fails (e.g., Mac permissions), print everything to the console
            print(f"\n--- FAILED TO SAVE FILE (OS Permission Error) ---")
            print(f"Error: {e}")
            print("The data was scraped successfully. The complete CSV output is printed below.\n")
            print("--- BEGIN CSV DATA ---")
            print(combined_df.to_csv(index=False))
            print("--- END CSV DATA ---")
    else:
        print("\n\n--- Scraper finished. No data was returned for any of the schools/days provided. ---")