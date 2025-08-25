import streamlit as st
import pandas as pd
from datetime import datetime, date
import asyncio
import aiohttp
import time

async def fetch_project_tasks(session, project_id):
    url = f"https://tasking-manager-production-api.hotosm.org/api/v2/projects/{project_id}/tasks/"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        st.error(f"Error fetching project tasks: {e}")
        return None

async def fetch_task_details(session, project_id, task_id):
    url = f"https://tasking-manager-production-api.hotosm.org/api/v2/projects/{project_id}/tasks/{task_id}/"
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        return None

def parse_datetime(date_string):
    if not date_string:
        return None
    return datetime.strptime(date_string.rstrip('Z'), "%Y-%m-%dT%H:%M:%S.%f%z")

async def filter_tasks_by_date(project_id, from_date, to_date, progress_bar, stats_container):
    async with aiohttp.ClientSession() as session:
        tasks_data = await fetch_project_tasks(session, project_id)
        if not tasks_data:
            return None
        
        task_ids = [feature['properties']['taskId'] for feature in tasks_data['features']]
        total_tasks = len(task_ids)
        
        stats_container.metric("Total Tasks Found", total_tasks)
        
        filtered_tasks = []
        start_time = time.time()
        
        semaphore = asyncio.Semaphore(10)
        
        async def process_task(task_id, index):
            async with semaphore:
                progress_bar.progress((index + 1) / total_tasks)
                task_details = await fetch_task_details(session, project_id, task_id)

                if task_details:
                    last_updated = parse_datetime(task_details['lastUpdated'])
                    if last_updated:
                        last_updated_date = last_updated.date()
                        if from_date <= last_updated_date <= to_date:
                            return {
                                'taskId': task_details['taskId'],
                                'projectId': task_details['projectId'],
                                'taskStatus': task_details['taskStatus'],
                                'lastUpdated': task_details['lastUpdated'],
                                'lastUpdatedBy': task_details['taskHistory'][0]['actionBy'] if task_details.get('taskHistory') and len(task_details['taskHistory']) > 0 else ''
                            }
                return None
        
        tasks = await asyncio.gather(*[process_task(task_id, i) for i, task_id in enumerate(task_ids)])
        filtered_tasks = [task for task in tasks if task is not None]
        
        elapsed_time = time.time() - start_time
        stats_container.metric("Fetch Time", f"{elapsed_time:.1f}s")
        stats_container.metric("Filtered Tasks", len(filtered_tasks))
        
        return filtered_tasks

def main():
    st.title("TM Project Task Filter")
    
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])
    
    with col1:
        project_id = st.text_input("Project ID", value="24229")
    
    with col2:
        from_date = st.date_input("From Date", value=date(2025, 8, 1))
    
    with col3:
        to_date = st.date_input("To Date", value=date.today())
    
    with col4:
        st.write("")
        st.write("")
        filter_button = st.button("Filter Tasks", use_container_width=True)
    
    stats_col1, stats_col2, stats_col3 = st.columns(3)
    
    if filter_button:
        try:
            project_id_int = int(project_id)
        except ValueError:
            st.error("Project ID must be a number")
            return
            
        if from_date > to_date:
            st.error("From date cannot be later than To date")
            return
        
        st.info(f"Fetching tasks for project {project_id_int} from {from_date} to {to_date}")
        
        progress_bar = st.progress(0)
        
        filtered_tasks = asyncio.run(filter_tasks_by_date(project_id_int, from_date, to_date, progress_bar, stats_col1))
        progress_bar.empty()
        
        if filtered_tasks:
            df = pd.DataFrame(filtered_tasks)
            df['lastUpdated'] = pd.to_datetime(df['lastUpdated'].str.rstrip('Z'), format='%Y-%m-%dT%H:%M:%S.%f%z')
            
            earliest_lock = df['lastUpdated'].min()
            latest_lock = df['lastUpdated'].max()
            print(earliest_lock, latest_lock)

            with stats_col2:
                st.metric("Status Distribution", "")
                status_counts = df['taskStatus'].value_counts()
                for status, count in status_counts.items():
                    st.write(f"{status}: {count}")
            
            with stats_col3:
                st.write("Earliest Lock", earliest_lock.strftime("%Y-%m-%d %H:%M"))
                st.write("Latest Lock", latest_lock.strftime("%Y-%m-%d %H:%M"))

            st.dataframe(df, use_container_width=True)
            
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"tasks_project_{project_id}_{from_date}_to_{to_date}.csv",
                mime="text/csv"
            )
        else:
            st.warning("No tasks found matching the date criteria")

if __name__ == "__main__":
    main()
