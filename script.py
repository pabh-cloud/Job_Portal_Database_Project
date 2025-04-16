import streamlit as st
import mysql.connector
import pandas as pd
import streamlit as st
import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import json
import uuid  # Added for generating unique IDs

# Function to create a database connection
def create_connection():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='@#nkjnkjnkj#29',#Put Your Root Password Here
            database='job_portal_sample'
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"Database Connection Error: {err}")
        return None

# Function to check if a table exists
def table_exists(conn, table_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            return cursor.fetchone() is not None
    except mysql.connector.Error as err:
        st.error(f"Error checking table: {err}")
        return False

# Function to get unique values for a column
def get_unique_values(column):
    conn = create_connection()
    if not conn:
        return []
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT DISTINCT `{column}` FROM Jobs_Sample ORDER BY `{column}`")
            return [item[0] for item in cursor.fetchall() if item[0]]
    except mysql.connector.Error as err:
        st.error(f"Error fetching unique values: {err}")
        return []
    finally:
        conn.close()

# Function to get common job roles from the database
def get_job_roles():
    conn = create_connection()
    if not conn:
        return []
    
    try:
        with conn.cursor() as cursor:
            # Get the most common job titles from the database
            cursor.execute("""
                SELECT `Job Title` FROM Jobs_Sample 
                GROUP BY `Job Title` 
                ORDER BY COUNT(*) DESC 
                LIMIT 20
            """)
            return [item[0] for item in cursor.fetchall() if item[0]]
    except mysql.connector.Error as err:
        st.error(f"Error fetching job roles: {err}")
        # Return some common job roles as fallback
        return ["Software Engineer", "Data Scientist", "Product Manager", 
                "UX Designer", "Marketing Specialist", "Sales Representative",
                "Project Manager", "Business Analyst", "DevOps Engineer",
                "Customer Support"]
    finally:
        conn.close()

# Function to parse salary range to numeric values for filtering
def parse_salary_range(salary_text):
    try:
        # Remove currency symbols and text, extract numbers
        salary_text = salary_text.replace('$', '').replace('k', '000').replace(',', '')
        parts = [s for s in salary_text.split() if s.isdigit() or s.replace('.', '', 1).isdigit()]
        if len(parts) >= 2:  # Has min and max
            return float(parts[0]), float(parts[1])
        elif len(parts) == 1:  # Has just one number
            return float(parts[0]), float(parts[0])
        else:
            return None, None
    except:
        return None, None

# Advanced job fetching with multiple filters and sorting
def fetch_filtered_jobs(
    job_title="", 
    locations=None, 
    companies=None, 
    work_types=None, 
    experience_levels=None,
    min_salary=None,
    max_salary=None,
    date_posted=None,
    sort_by="Job Title",
    sort_order="ASC",
    offset=0, 
    limit=10
):
    conn = create_connection()
    if not conn:
        return [], 0
    
    if not table_exists(conn, "Jobs_Sample"):
        st.error("Table 'Jobs_Sample' does not exist.")
        return [], 0
    
    try:
        with conn.cursor(dictionary=True) as cursor:
            # Build WHERE clause based on filters
            conditions = []
            params = []
            
            if job_title:
                conditions.append("`Job Title` LIKE %s")
                params.append(f"%{job_title}%")
                
            if locations and locations != ['All']:
                placeholders = ', '.join(['%s'] * len(locations))
                conditions.append(f"`location` IN ({placeholders})")
                params.extend(locations)
                
            if companies and companies != ['All']:
                placeholders = ', '.join(['%s'] * len(companies))
                conditions.append(f"`Company` IN ({placeholders})")
                params.extend(companies)
                
            if work_types and work_types != ['All']:
                placeholders = ', '.join(['%s'] * len(work_types))
                conditions.append(f"`Work Type` IN ({placeholders})")
                params.extend(work_types)
                
            if experience_levels and experience_levels != ['All']:
                placeholders = ', '.join(['%s'] * len(experience_levels))
                conditions.append(f"`Experience` IN ({placeholders})")
                params.extend(experience_levels)
            
            # Handle date posted filtering
            if date_posted:
                days_ago = {
                    "Last 24 hours": 1,
                    "Last 3 days": 3, 
                    "Last week": 7,
                    "Last month": 30
                }.get(date_posted)
                
                if days_ago:
                    conditions.append("`Date Posted` >= %s")
                    params.append((datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d'))
            
            # Build the final query
            where_clause = " AND ".join(conditions) if conditions else "1=1"
            
            # Get total count of matching records
            count_query = f"SELECT COUNT(*) FROM Jobs_Sample WHERE {where_clause}"
            cursor.execute(count_query, params)
            total_records = cursor.fetchone()['COUNT(*)']
            
            # Fetch the filtered records with sorting
            query = f"""
                SELECT *, ROW_NUMBER() OVER() as row_id FROM Jobs_Sample 
                WHERE {where_clause}
                ORDER BY `{sort_by}` {sort_order}
                LIMIT %s OFFSET %s
            """
            full_params = params + [limit, offset]
            cursor.execute(query, full_params)
            results = cursor.fetchall()
            
            # Apply salary filtering in Python (since salary might be stored as text)
            if min_salary is not None or max_salary is not None:
                filtered_results = []
                for job in results:
                    # Parse the salary range from the job
                    job_min, job_max = parse_salary_range(job['Salary Range'])
                    
                    # Skip jobs where we couldn't parse salary
                    if job_min is None and job_max is None:
                        continue
                        
                    # Apply salary filters
                    if min_salary is not None and job_max is not None and job_max < min_salary:
                        continue
                    if max_salary is not None and job_min is not None and job_min > max_salary:
                        continue
                        
                    filtered_results.append(job)
                
                # Recalculate total after salary filtering
                total_filtered = len(filtered_results)
                
                return filtered_results, total_filtered
            
            return results, total_records
            
    except mysql.connector.Error as err:
        st.error(f"Error filtering jobs: {err}")
        return [], 0
    finally:
        conn.close()

# Save and load search preferences
def save_preferences(preferences, name):
    try:
        saved_prefs = json.loads(st.session_state.get('saved_preferences', '{}'))
        saved_prefs[name] = preferences
        st.session_state['saved_preferences'] = json.dumps(saved_prefs)
        return True
    except Exception as e:
        st.error(f"Error saving preferences: {e}")
        return False

def load_preferences(name):
    try:
        saved_prefs = json.loads(st.session_state.get('saved_preferences', '{}'))
        return saved_prefs.get(name, {})
    except Exception as e:
        st.error(f"Error loading preferences: {e}")
        return {}

# Streamlit App UI
st.title("🔍 Job Portal")

# Initialize session state for saved searches
if 'saved_preferences' not in st.session_state:
    st.session_state['saved_preferences'] = '{}'

# Sidebar Search and Filter Options
st.sidebar.header("Search & Filter Options")

# Load saved search if requested
saved_searches = json.loads(st.session_state.get('saved_preferences', '{}'))
saved_search_names = list(saved_searches.keys())

if saved_search_names:
    selected_saved_search = st.sidebar.selectbox(
        "Load saved search",
        ["None"] + saved_search_names
    )
    
    if selected_saved_search != "None":
        # Load the saved search preferences
        preferences = load_preferences(selected_saved_search)
        
        # Apply saved preferences to session state
        for key, value in preferences.items():
            st.session_state[key] = value

# Get job roles for dropdown
job_roles = [""] + get_job_roles()

# Job title section with dropdown and custom input
st.sidebar.subheader("Job Title")
job_title_type = st.sidebar.radio(
    "Select job title input method:",
    ["Choose from list", "Enter custom title"],
    horizontal=True,
    index=0 if not st.session_state.get('custom_job_title') else 1
)

if job_title_type == "Choose from list":
    job_title_search = st.sidebar.selectbox(
        "Select a job role",
        job_roles,
        index=job_roles.index(st.session_state.get('job_title_search', '')) if st.session_state.get('job_title_search', '') in job_roles else 0
    )
    # Clear the custom job title when using dropdown
    st.session_state['custom_job_title'] = ''
else:
    # Use text input for custom job title
    job_title_search = st.sidebar.text_input(
        "Enter job title",
        value=st.session_state.get('custom_job_title', '')
    )
    # Store the custom job title
    st.session_state['custom_job_title'] = job_title_search


# Locations filter
all_locations = ["All"] + get_unique_values("location")
locations_filter = st.sidebar.multiselect("Locations", 
                                           all_locations, 
                                           default=st.session_state.get('locations_filter', ["All"]))

# Companies filter
all_companies = ["All"] + get_unique_values("Company")
companies_filter = st.sidebar.multiselect("Companies", 
                                           all_companies, 
                                           default=st.session_state.get('companies_filter', ["All"]))

# Work Types filter
all_work_types = ["All"] + get_unique_values("Work Type") 
work_types_filter = st.sidebar.multiselect("Work Types", 
                                           all_work_types, 
                                           default=st.session_state.get('work_types_filter', ["All"]))

# Experience Levels filter
all_experience_levels = ["All"] + get_unique_values("Experience")
experience_filter = st.sidebar.multiselect("Experience", 
                                           all_experience_levels, 
                                           default=st.session_state.get('experience_filter', ["All"]))

# Date posted filter
date_posted_options = ["Any time", "Last 24 hours", "Last 3 days", "Last week", "Last month"]
date_posted_filter = st.sidebar.selectbox("Date Posted", 
                                          date_posted_options,
                                          index=date_posted_options.index(st.session_state.get('date_posted_filter', "Any time")))

# Salary range filter
st.sidebar.subheader("Salary Range")
min_salary = st.sidebar.number_input("Minimum Salary (K)", 
                                     min_value=0, 
                                     value=st.session_state.get('min_salary', 0))
max_salary = st.sidebar.number_input("Maximum Salary (K)", 
                                     min_value=0, 
                                     value=st.session_state.get('max_salary', 0))

# Convert salary inputs to thousands
min_salary = min_salary * 1000 if min_salary > 0 else None
max_salary = max_salary * 1000 if max_salary > 0 else None

# Sorting options
st.sidebar.subheader("Sort Results")
sort_options = ["Job Title", "Company", "Date Posted", "Experience"]
sort_by = st.sidebar.selectbox("Sort by", 
                              sort_options,
                              index=sort_options.index(st.session_state.get('sort_by', "Job Title")))
sort_order = st.sidebar.selectbox("Order", 
                                 ["Ascending", "Descending"],
                                 index=["Ascending", "Descending"].index(st.session_state.get('sort_order', "Ascending")))

# Apply button for filtering
apply_filters = st.sidebar.button("Apply Filters")

# Save search preferences
with st.sidebar.expander("Save This Search"):
    search_name = st.text_input("Name for this search")
    save_search = st.button("Save")
    
    if save_search and search_name:
        # Collect current preferences
        current_preferences = {
            'job_title_search': job_title_search,
            'locations_filter': locations_filter,
            'companies_filter': companies_filter,
            'work_types_filter': work_types_filter,
            'experience_filter': experience_filter,
            'date_posted_filter': date_posted_filter,
            'min_salary': int(min_salary/1000) if min_salary else 0,
            'max_salary': int(max_salary/1000) if max_salary else 0,
            'sort_by': sort_by,
            'sort_order': sort_order
        }
        
        if save_preferences(current_preferences, search_name):
            st.sidebar.success(f"Search '{search_name}' saved!")

# Pagination Controls
limit = 10
page = st.sidebar.number_input("Page", min_value=1, value=1)
offset = (page - 1) * limit

# Reset all filters
if st.sidebar.button("Reset All Filters"):
    # Clear session state for all filter values
    for key in ['job_title_search', 'locations_filter', 'companies_filter', 
                'work_types_filter', 'experience_filter', 'date_posted_filter',
                'min_salary', 'max_salary', 'sort_by', 'sort_order']:
        if key in st.session_state:
            del st.session_state[key]
    st.experimental_rerun()

# Store current filter settings in session state
if apply_filters:
    st.session_state['job_title_search'] = job_title_search
    st.session_state['locations_filter'] = locations_filter
    st.session_state['companies_filter'] = companies_filter
    st.session_state['work_types_filter'] = work_types_filter
    st.session_state['experience_filter'] = experience_filter
    st.session_state['date_posted_filter'] = date_posted_filter
    st.session_state['min_salary'] = int(min_salary/1000) if min_salary else 0
    st.session_state['max_salary'] = int(max_salary/1000) if max_salary else 0
    st.session_state['sort_by'] = sort_by
    st.session_state['sort_order'] = sort_order

# Display job listings
st.header("📌 Available Jobs")

# Process search/filter parameters
date_filter = None if date_posted_filter == "Any time" else date_posted_filter
sort_direction = "ASC" if sort_order == "Ascending" else "DESC"

# Fetch filtered jobs
jobs, total_filtered = fetch_filtered_jobs(
    job_title=job_title_search,
    locations=None if 'All' in locations_filter else locations_filter,
    companies=None if 'All' in companies_filter else companies_filter,
    work_types=None if 'All' in work_types_filter else work_types_filter,
    experience_levels=None if 'All' in experience_filter else experience_filter,
    min_salary=min_salary,
    max_salary=max_salary,
    date_posted=date_filter,
    sort_by=sort_by,
    sort_order=sort_direction,
    offset=offset,
    limit=limit
)

# Display filter summary and results count
active_filters = []
if job_title_search:
    active_filters.append(f"Title: '{job_title_search}'")
if locations_filter and 'All' not in locations_filter:
    active_filters.append(f"Locations: {', '.join(locations_filter)}")
if companies_filter and 'All' not in companies_filter:
    active_filters.append(f"Companies: {', '.join(companies_filter)}")
if work_types_filter and 'All' not in work_types_filter:
    active_filters.append(f"Work Types: {', '.join(work_types_filter)}")
if experience_filter and 'All' not in experience_filter:
    active_filters.append(f"Experience: {', '.join(experience_filter)}")
if date_posted_filter != "Any time":
    active_filters.append(f"Posted: {date_posted_filter}")
if min_salary:
    active_filters.append(f"Min Salary: ${int(min_salary/1000)}k")
if max_salary:
    active_filters.append(f"Max Salary: ${int(max_salary/1000)}k")

if active_filters:
    st.write(f"🔍 Filtered by: {' | '.join(active_filters)}")

st.write(f"Found {total_filtered} job(s)")

# Show pagination info
max_pages = (total_filtered // limit) + (1 if total_filtered % limit > 0 else 0)
if max_pages > 1:
    st.write(f"Page {page} of {max_pages}")

# Display jobs
if jobs:
    # Add view options
    view_mode = st.radio("View mode:", ("Cards", "Table"), horizontal=True)
    
    if view_mode == "Cards":
        for i, job in enumerate(jobs):
            # Use index and row_id (if available) to ensure unique keys
            unique_id = job.get('row_id', i)
            
            with st.expander(f"{job['Job Title']} - {job['Company']} ({job['location']})"):
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.write(f"**🏢 Company:** {job['Company']}")
                    st.write(f"**📍 Location:** {job['location']}")
                    st.write(f"**💰 Salary:** {job['Salary Range']}")
                    st.write(f"**⏱️ Work Type:** {job['Work Type']}")
                    st.write(f"**🧠 Experience Required:** {job['Experience']}")
                    
                    if 'Date Posted' in job and job['Date Posted']:
                        st.write(f"**📅 Posted on:** {job['Date Posted']}")
                
                with col2:
                    # Use unique_id to ensure button keys are unique
                    st.button("Apply Now", key=f"apply_{unique_id}")
                
                st.markdown("### Job Description")
                st.write(f"{job['Job Description']}")
                st.markdown("---")
    else:
        # Table view
        df = pd.DataFrame(jobs)
        
        # Reorder and select columns for display
        if not df.empty:
            # Remove row_id from display if it exists
            if 'row_id' in df.columns:
                df = df.drop(columns=['row_id'])
                
            display_cols = ['Job Title', 'Company', 'location', 'Work Type', 
                           'Experience', 'Salary Range']
            # Only include columns that exist in the dataframe
            display_cols = [col for col in display_cols if col in df.columns]
            st.dataframe(df[display_cols], use_container_width=True)
else:
    st.warning("No jobs found matching your criteria. Try adjusting your filters.")

# Add a download button for search results
if jobs:
    jobs_df = pd.DataFrame(jobs)
    
    # Remove row_id from download if it exists
    if 'row_id' in jobs_df.columns:
        jobs_df = jobs_df.drop(columns=['row_id'])
    
    @st.cache_data
    def convert_df_to_csv(df):
        return df.to_csv(index=False).encode('utf-8')
    
    csv = convert_df_to_csv(jobs_df)
    st.download_button(
        label="Download search results as CSV",
        data=csv,
        file_name="job_search_results.csv",
        mime="text/csv",
    )

# Advanced analytics section
if jobs:
    with st.expander("📊 Job Market Analytics"):
        st.write("### Insights from current search results")
        
        # Sample analytics based on the filtered results
        all_filtered_jobs, _ = fetch_filtered_jobs(
            job_title=job_title_search,
            locations=None if 'All' in locations_filter else locations_filter,
            companies=None if 'All' in companies_filter else companies_filter,
            work_types=None if 'All' in work_types_filter else work_types_filter,
            experience_levels=None if 'All' in experience_filter else experience_filter,
            min_salary=min_salary,
            max_salary=max_salary,
            date_posted=date_filter,
            limit=1000,  # Get more for analytics
            offset=0
        )
        
        if all_filtered_jobs:
            df = pd.DataFrame(all_filtered_jobs)
            
            # Remove row_id from analytics if it exists
            if 'row_id' in df.columns:
                df = df.drop(columns=['row_id'])
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Count by work type
                if 'Work Type' in df.columns:
                    work_type_counts = df['Work Type'].value_counts()
                    st.write("#### Work Type Distribution")
                    st.bar_chart(work_type_counts)
            
            with col2:
                # Count by location
                if 'location' in df.columns:
                    location_counts = df['location'].value_counts().head(10)
                    st.write("#### Top Locations")
                    st.bar_chart(location_counts)

# Function to create a database connection
def create_connection():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='@#deeproot07@#29',
            database='job_portal_sample'
        )
        return conn
    except mysql.connector.Error as err:
        st.error(f"Database Connection Error: {err}")
        return None

# Function to check if a table exists
def table_exists(conn, table_name):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW TABLES LIKE %s", (table_name,))
            return cursor.fetchone() is not None
    except mysql.connector.Error as err:
        st.error(f"Error checking table: {err}")
        return False

# Function to fetch jobs with pagination
def fetch_jobs(offset=0, limit=10):
    conn = create_connection()
    if not conn:
        return []
    
    if not table_exists(conn, "Jobs_Sample"):
        st.error("Table 'Jobs_Sample' does not exist.")
        return []
    
    try:
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM Jobs_Sample LIMIT %s OFFSET %s", (limit, offset))
            return cursor.fetchall()
    except mysql.connector.Error as err:
        st.error(f"Error fetching jobs: {err}")
        return []
    finally:
        conn.close()

# Function to count total jobs
def count_total_jobs():
    conn = create_connection()
    if not conn:
        return 0
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM Jobs_Sample")
            return cursor.fetchone()[0]
    except mysql.connector.Error as err:
        st.error(f"Error counting jobs: {err}")
        return 0
    finally:
        conn.close()

# Function to filter jobs based on user input
def search_jobs(column, search_value):
    conn = create_connection()
    if not conn:
        return []
    
    if not table_exists(conn, "Jobs_Sample"):
        st.error("Table 'Jobs_Sample' does not exist.")
        return []
    
    try:
        with conn.cursor(dictionary=True) as cursor:
            query = f"SELECT * FROM Jobs_Sample WHERE `{column}` LIKE %s"
            cursor.execute(query, (f"%{search_value}%",))
            return cursor.fetchall()
    except mysql.connector.Error as err:
        st.error(f"Error searching jobs: {err}")
        return []
    finally:
        conn.close()

# Function to count filtered jobs
def count_filtered_jobs(column, search_value):
    conn = create_connection()
    if not conn:
        return 0
    
    if not table_exists(conn, "Jobs_Sample"):
        st.error("Table 'Jobs_Sample' does not exist.")
        return 0
    
    try:
        with conn.cursor() as cursor:
            query = f"SELECT COUNT(*) FROM Jobs_Sample WHERE `{column}` LIKE %s"
            cursor.execute(query, (f"%{search_value}%",))
            return cursor.fetchone()[0]
    except mysql.connector.Error as err:
        st.error(f"Error counting filtered jobs: {err}")
        return 0
    finally:
        conn.close()

# Streamlit App UI
st.title("🔍 Job Portal")

# Sidebar Search Options
st.sidebar.header("Search Jobs")
columns_map = {
    "Job Title": "Job Title",
    "Location": "location",
    "Company": "Company",
    "Work Type": "Work Type",
    "Experience": "Experience"
}
search_column_label = st.sidebar.selectbox("Search by", list(columns_map.keys()))
search_column = columns_map[search_column_label]
search_input = st.sidebar.text_input(f"Enter {search_column_label}")
search_button = st.sidebar.button("Search")

# Pagination Controls
total_jobs = count_total_jobs()
limit = 10
page = st.sidebar.number_input("Page", 1, max(1, (total_jobs // limit) + 1))
offset = (page - 1) * limit

# Show Job Listings
st.subheader("📌 Available Jobs")

if search_button and search_input:
    total_filtered_jobs = count_filtered_jobs(search_column, search_input)
    st.sidebar.write(f"🔍 Found {total_filtered_jobs} job(s) for '{search_input}'")
    jobs = search_jobs(search_column, search_input)
else:
    jobs = fetch_jobs(offset, limit)

if jobs:
    for job in jobs:
        with st.expander(f"{job['Job Title']} - {job['location']} ({job['Work Type']})"):
            st.write(f"🏢 Company: {job['Company']}")
            st.write(f"💰 Salary: {job['Salary Range']}")
            st.write(f"📄 Description: {job['Job Description']}")
            st.write("---")
else:
    st.warning("No jobs found. Try different search criteria.")

# Optional: Display jobs in tabular format
if st.checkbox("Show jobs as table"):
    df = pd.DataFrame(jobs)
    st.dataframe(df)
 # type: ignore