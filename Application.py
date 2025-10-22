import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, time
import numpy as np
import calendar

# --- Configuration (Aanpassen van de App) ---

st.set_page_config(layout="wide", page_title="ArcelorMittal SIDGAL Energy Dashboard")

# Define the absolute file path 
EXCEL_FILE = 'Time_Series_Analyse_Mean_Only.xlsx'

# --- MAPPINGS ---

SIDGAL_DISPLAY_MAPPING = {
    'sdg1': 'Sidgal 1', 'sdg2': 'Sidgal 2', 
    'sdg3': 'Sidgal 3', 'sdg4': 'Sidgal 4'
}
SIDGAL_LINES = list(SIDGAL_DISPLAY_MAPPING.keys())
TIME_AGGREGATIONS_ENGLISH = ['Hourly', 'Daily', 'Weekly', 'Monthly', 'Yearly']

AGGREGATION_MAPPING = {
    'Hourly': 'Uur', 'Daily': 'Dag', 'Weekly': 'Week', 
    'Monthly': 'Maand', 'Yearly': 'Jaar'
}

# Unit Mapping (voor Y-as label)
UNIT_MAPPING = {
    'stoom': 'ton/h',
    'vermogen actief': 'MW', 'sgal3b': 'MW', '6kv': 'MW',
    'klasse a': 'm³/h', 'klasse b': 'm³/h', 'drinkwater': 'm³/h', 'kanaalwater': 'm³/h',
    'waterstof': 'Nm³/h', 'stikstof': 'Nm³/h', 'aardgas': 'Nm³/h', 
    'perslucht': 'Nm³/h', 'cokesgas': 'Nm³/h', 'instrumentatielucht': 'Nm³/h',
    'debiet': 'Nm³/h', 'gas': 'Nm³/h',
}

# Map van de 'rate' unit naar de 'volume/total' unit
RATE_TO_VOLUME_UNIT = {
    'ton/h': 'ton', 'MW': 'MWh', 'm³/h': 'm³', 'Nm³/h': 'Nm³', 'Units': 'Units'
}

# CONSOLIDATION MAPPING - GEBASEERD OP DE EXACTE LIJST VAN DE GEBRUIKER
CONSOLIDATION_MAPPING = {
    # Gassen (Nm³/h)
    'aardgas': 'Aardgas Debiet Totaal',
    'waterstof': 'Waterstof Debiet Totaal',
    'stikstof ld sidgal 1': 'Stikstof Debiet Totaal',
    'stikstof ld sidgal 2': 'Stikstof Debiet Totaal',
    'stikstof ld blaasmessen': 'Stikstof Debiet Totaal',
    'stikstof sidgal 4': 'Stikstof Debiet Totaal',
    'cokesgas': 'Cokesgas Debiet Totaal',
    'perslucht sidgal 1': 'Perslucht Debiet Totaal',
    'perslucht sidgal 2': 'Perslucht Debiet Totaal', 
    'perslucht sidgal 3': 'Perslucht Debiet Totaal', 
    'perslucht sidgal 4': 'Perslucht Debiet Totaal',
    
    # Instrumentatielucht
    'instrumentatielucht sidgal 1': 'Instrumentatielucht Totaal', 
    'perslucht sidgal 2 instrumentatielucht': 'Instrumentatielucht Totaal',
    'perslucht sidgal 3 instrumentatielucht': 'Instrumentatielucht Totaal',
    'instrumentenlucht sidgal 4': 'Instrumentatielucht Totaal',

    # Water (m³/h)
    'drinkwater': 'Drinkwater Debiet Totaal',
    'kanaalwater': 'Kanaalwater Debiet Totaal',
    'klasse a': 'Water Klasse A Totaal',
    'klasse b': 'Water Klasse B Totaal', 

    # Overig (ton/h & MW)
    'stoom': 'Stoom Massa Totaal', # Vangt alle stoom sensoren
    'vermogen actief (enp)': 'Elektriciteit Totaal (MW)', # Vangt S2/S3 6kV
    'sgal3b': 'Elektriciteit Totaal (MW)', # Vangt SGAl3B
}


def get_sensor_unit(sensor_name):
    """Determines the unit for an INDIVIDUAL sensor name."""
    name_lower = sensor_name.lower()
    for keyword, unit in UNIT_MAPPING.items():
        if keyword in name_lower:
            return unit
    return 'Units' 

def get_consolidated_name(sensor_name):
    """Retrieves the consolidated column name."""
    name_lower = sensor_name.lower()
    for keyword, consolidated_name in CONSOLIDATION_MAPPING.items():
        # Some keywords are part of the sensor names. This is crucial.
        if keyword in name_lower:
            return consolidated_name
    return None

# --- Data Loading Function ---
@st.cache_data
def load_data(sheet_name):
    """Loads a specific sheet from the Excel file."""
    try:
        df = pd.read_excel(EXCEL_FILE, sheet_name=sheet_name)
        df.iloc[:, 0] = pd.to_datetime(df.iloc[:, 0])
        return df
    except (FileNotFoundError, ValueError) as e:
        return pd.DataFrame()
    except Exception as e:
        st.error(f"An unexpected error occurred while loading data: {e}")
        return pd.DataFrame()

# --- Consolidatie Functie ---
@st.cache_data
def consolidate_data(aggregation_sheet_part):
    """Loads all 4 SIDGAL sheets for an aggregation and sums the consolidated sensors."""
    
    all_dfs = {}
    time_col = None
    
    # 1. Load all SIDGAL lines
    for key in SIDGAL_LINES:
        sheet_name = f"{key} - {aggregation_sheet_part}"
        df = load_data(sheet_name)
        if not df.empty:
            all_dfs[key] = df
            if time_col is None:
                time_col = df.columns[0]
    
    if not all_dfs:
        return pd.DataFrame()
    
    # 2. Determine a common time column
    unique_dates = pd.concat([df[time_col] for df in all_dfs.values()]).unique()
    consolidated_df = pd.DataFrame({time_col: unique_dates})
    
    # 3. Add all dataframes to the consolidated_df
    for key, df in all_dfs.items():
        # Rename sensor columns to make them unique
        sensor_cols = df.columns.drop(time_col)
        rename_map = {col: f"{col}_{key}" for col in sensor_cols}
        
        df_to_merge = df[[time_col] + sensor_cols.tolist()].rename(columns=rename_map)
        consolidated_df = pd.merge(consolidated_df, df_to_merge, on=time_col, how='outer')

    # 4. Consolidate by Energy Group
    final_consolidated_df = consolidated_df[[time_col]].copy()
    all_cols = consolidated_df.columns.drop(time_col)
    
    for consolidated_name in set(CONSOLIDATION_MAPPING.values()):
        # Find all columns belonging to this consolidated name
        original_cols_to_sum = [
            col for col in all_cols 
            if get_consolidated_name(col.rsplit('_', 1)[0]) == consolidated_name # Slice off the SIDGAL key
        ]

        if original_cols_to_sum:
            # Sum the columns
            final_consolidated_df[consolidated_name] = consolidated_df[original_cols_to_sum].sum(axis=1, skipna=True)
            
    final_consolidated_df = final_consolidated_df.sort_values(by=time_col).reset_index(drop=True)
    
    if final_consolidated_df.columns.size == 1:
        return pd.DataFrame()
        
    return final_consolidated_df.dropna(subset=[time_col])


# --- Consumption Calculation ---
def calculate_consumption(data_df_filtered, selected_aggregation_display, time_column, sensor_cols):
    """
    Calculates the total consumption (integration) by multiplying the average 'rate' 
    by the exact time interval between data points.
    """
    df_consumption = data_df_filtered.copy()
    
    # 1. Calculate the exact time interval in hours for each row.
    df_consumption['Time_Delta_Hours'] = df_consumption[time_column].diff().dt.total_seconds() / 3600
    
    # Determine the default hours for the first row
    default_hours = None
    
    if len(df_consumption) == 1:
        # If there is only one point, we determine the duration from the calendar
        start_date = data_df_filtered[time_column].iloc[0]
        if selected_aggregation_display == 'Monthly':
            days_in_month = calendar.monthrange(start_date.year, start_date.month)[1]
            default_hours = days_in_month * 24
        elif selected_aggregation_display == 'Yearly':
            is_leap = calendar.isleap(start_date.year)
            default_hours = (366 if is_leap else 365) * 24
        elif selected_aggregation_display == 'Hourly': default_hours = 1
        elif selected_aggregation_display == 'Daily': default_hours = 24
        elif selected_aggregation_display == 'Weekly': default_hours = 24 * 7
    elif len(df_consumption) > 1:
        # Use the mode of the series as an estimate for the first unknown delta.
        mode_hours = df_consumption['Time_Delta_Hours'].mode()
        default_hours = mode_hours.iloc[0] if not mode_hours.empty else 1 

    # Fill the NaN value of the first row with the calculated or estimated default_hours
    if default_hours is not None:
        df_consumption['Time_Delta_Hours'] = df_consumption['Time_Delta_Hours'].fillna(default_hours)
    else:
        # Fallback 
        df_consumption['Time_Delta_Hours'] = df_consumption['Time_Delta_Hours'].fillna(1)

    # 2. Multiply the average value by the dynamic hours interval
    for col in sensor_cols:
        rate_unit = get_sensor_unit(col)
        
        # Multiply only 'rate' sensors (* /h)
        if '/h' in rate_unit or rate_unit == 'MW': 
             df_consumption[col] = df_consumption[col] * df_consumption['Time_Delta_Hours']
        
    # Remove the temporary column
    df_consumption = df_consumption.drop(columns=['Time_Delta_Hours'])
    
    return df_consumption

# --- Plotting Functions ---

def plot_data(df, time_col, sensor_cols, selected_agg_display, selected_sidgal_display, smoothing_window, is_consumption):
    """Draws the graphs, split by unit."""
    
    # Determine the units for each selected sensor
    if is_consumption and selected_sidgal_display in ["All SIDGAL Lines", "All SIDGAL Lines (Avg)"]: # Vertaald
        # For consolidated data and consumption: use the VOLUME unit in the map
        sensor_unit_map = {
            col: RATE_TO_VOLUME_UNIT.get(get_sensor_unit(col), get_sensor_unit(col)) 
            for col in sensor_cols
        }
    else:
        # Otherwise: use the standard RATE unit
        sensor_unit_map = {col: get_sensor_unit(col) for col in sensor_cols}
    
    # Data Transformation (Melt)
    df_long = df.melt(
        id_vars=[time_col],
        value_vars=sensor_cols,
        var_name='Sensor',
        value_name='Output'
    )
    
    # Add the unit column
    df_long['Unit'] = df_long['Sensor'].map(sensor_unit_map)
        
    is_yearly = selected_agg_display == 'Yearly'

    # Group the data by Unit
    for unit, unit_group in df_long.groupby('Unit'):
        
        # Determine the correct title and y-axis label
        if selected_sidgal_display == "All SIDGAL Lines (Avg)": # Vertaald
            y_axis_label = f"Average Output ({unit})"
            title_prefix = "Average Site Output"
        elif is_consumption:
            y_axis_label = f"Total Consumption ({unit})"
            title_prefix = "Total Consumption"
        else:
            y_axis_label = f"Average Output ({unit})"
            title_prefix = "Average Output"

        
        if is_yearly:
            # Bar Chart for Yearly
            fig = px.bar(
                unit_group,
                x='Sensor',
                y='Output',
                color='Sensor',
                title=f'{selected_sidgal_display} - Annual {title_prefix} in {unit}',
                labels={'Output': y_axis_label, 'Sensor': 'Sensor Name'}
            )
            fig.update_layout(xaxis={'categoryorder':'total descending'})
        else:
            # Line Chart for Time Series
            fig = px.line(
                unit_group,
                x=time_col,
                y='Output',
                color='Sensor',
                title=f'{selected_sidgal_display} - {selected_agg_display} {title_prefix} in {unit} (Smoothing: {smoothing_window})',
                labels={time_col: 'Time Period', 'Output': y_axis_label}
            )
            fig.update_traces(mode='lines' if smoothing_window > 1 or is_consumption else 'lines+markers') 

        # Customize the layout
        fig.update_layout(
            legend_title_text='Sensor',
            xaxis_title='Time Period' if not is_yearly else 'Sensor Name',
            yaxis_title=y_axis_label
        )

        st.markdown(f"### Data in {unit}")
        st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")

# --- Streamlit UI Logic ---

st.title("SIDGAL Energy Output Dashboard")
st.markdown("Use the controls on the left to select data, and switch tabs to view the Average Intensity or Total Consumption.")

# --- Sidebar Controls ---
st.sidebar.header("Data Selection Filters")

# 1. SIDGAL Line Selector
line_options = ['Site Total'] + SIDGAL_LINES 
selected_sidgal_key = st.sidebar.selectbox(
    "Select SIDGAL Line:",
    options=line_options,
    format_func=lambda x: SIDGAL_DISPLAY_MAPPING.get(x, x)
)

is_site_total = selected_sidgal_key == 'Site Total'

# 2. Time Aggregation Selector
selected_aggregation_display = st.sidebar.selectbox(
    "Select Time Aggregation:",
    options=TIME_AGGREGATIONS_ENGLISH
)
selected_aggregation_sheet_part = AGGREGATION_MAPPING[selected_aggregation_display]

# Initial Data Loading for Date Range determination
data_df = pd.DataFrame()
if not is_site_total:
    selected_sheet_name = f"{selected_sidgal_key} - {selected_aggregation_sheet_part}"
    data_df = load_data(selected_sheet_name)
    selected_sidgal_display = SIDGAL_DISPLAY_MAPPING[selected_sidgal_key]


# --- Dynamic Date/Time Filtering ---

# Load Site Data (simple version to determine min/max date)
if is_site_total:
    site_data_all_time = consolidate_data(selected_aggregation_sheet_part)
    if not site_data_all_time.empty:
        time_column = site_data_all_time.columns[0]
        min_date = site_data_all_time[time_column].min().date()
        max_date = site_data_all_time[time_column].max().date()
    else:
        min_date = datetime.now().date()
        max_date = datetime.now().date()
else:
    # Use the data of the individual line
    if not data_df.empty:
        time_column = data_df.columns[0]
        min_date = data_df[time_column].min().date()
        max_date = data_df[time_column].max().date()
    else:
        min_date = datetime.now().date()
        max_date = datetime.now().date()

# 3. Date Range Selector
st.sidebar.subheader("Time Period Filter")
start_date = st.sidebar.date_input('Start Date:', min_date, min_value=min_date, max_value=max_date)
end_date = st.sidebar.date_input('End Date:', max_date, min_value=min_date, max_value=max_date)

# --- Smoothing Logic (Gedefinieerd voor Site Total en Individuele Lijn) ---
smoothing_window = 1
is_yearly = selected_aggregation_display == 'Yearly'

st.sidebar.subheader("Visualization Options") 

if not is_yearly:
    if is_site_total:
        smoothing_window = st.sidebar.slider(
            'Data Smoothing (Moving Average Window) - Site:',
            min_value=1, max_value=20, value=1, step=1, 
            help="Choose a value greater than 1 to apply a moving average to the Total Site Average Output.", # Vertaald
            key='site_smoothing_slider'
        )
    else:
        smoothing_window = st.sidebar.slider(
            'Data Smoothing (Moving Average Window) - Line:',
            min_value=1, max_value=20, value=1, step=1, 
            help="Choose a value greater than 1 to apply a moving average and filter noise.", # Vertaald
            key='line_smoothing_slider'
        )
else:
    st.sidebar.markdown("_Smoothing is not applicable to yearly data._") # Vertaald

# --- Tabs voor Output ---
# Tabbladen voor de hoofdweergave: Grafieken en Quick-Check
tab_graphs, tab_quick_check = st.tabs(["📊 Graphs", "🔍 Data Quick-Check"]) # Vertaald


# --- Data Verwerking & Plotting in Tabbladen ---

# Hoofdtab 1: Grafieken
with tab_graphs:

    if is_site_total:
        # --- SITE TOTAL LOGICA ---
        if not site_data_all_time.empty:
            data_df_raw = site_data_all_time[
                (site_data_all_time[time_column].dt.date >= start_date) & 
                (site_data_all_time[time_column].dt.date <= end_date)
            ].copy()
            
            all_sensor_cols = data_df_raw.columns.drop(time_column)
            
            if not data_df_raw.empty and not all_sensor_cols.empty:
                
                # --- Sidebar Controls: Sensor Multi-Select ---
                selected_sensors = st.sidebar.multiselect(
                    'Select Energy Streams to Display (Graphs):',
                    options=all_sensor_cols.tolist(),
                    default=all_sensor_cols.tolist(),
                    key='site_sensor_select'
                )

                sensor_cols_filtered = [col for col in selected_sensors if col in all_sensor_cols]
                data_df_filtered = data_df_raw[[time_column] + sensor_cols_filtered].copy()
                
                if data_df_filtered.empty or data_df_filtered.columns.size == 1:
                    st.warning("No data available after selecting streams.") # Vertaald
                else:
                    
                    # 1. Bereken de Averaged Trend data (Smoothing)
                    data_df_trend = data_df_filtered.copy()
                    if smoothing_window > 1 and not is_yearly:
                        for col in sensor_cols_filtered:
                            data_df_trend[col] = data_df_trend[col].rolling(window=smoothing_window, min_periods=1).mean()
                            
                    # 2. Bereken de verbruikswaarden (Consumption)
                    data_df_consumption = calculate_consumption(data_df_filtered, selected_aggregation_display, time_column, sensor_cols_filtered)
                    
                    # --- Sub-Tab Navigatie voor Site Total ---
                    st.markdown(f"## Total Site: All SIDGAL Lines ({selected_aggregation_display})") # Vertaald
                    
                    sub_tab_avg, sub_tab_total = st.tabs(["📊 Average Output (Intensity)", "📈 Total Consumption (Volume)"])
                    
                    with sub_tab_avg:
                        # Plot de geconsolideerde TREND data (Average Output met smoothing)
                        plot_data(
                            data_df_trend, 
                            time_column, 
                            sensor_cols_filtered, 
                            selected_aggregation_display, 
                            "All SIDGAL Lines (Avg)", # Vertaald
                            smoothing_window=smoothing_window, 
                            is_consumption=False 
                        )

                    with sub_tab_total:
                        # Plot de geconsolideerde verbruikswaardes (geen smoothing nodig voor Total)
                        plot_data(
                            data_df_consumption, 
                            time_column, 
                            sensor_cols_filtered, 
                            selected_aggregation_display, 
                            "All SIDGAL Lines", # Vertaald
                            smoothing_window=1, 
                            is_consumption=True 
                        )
            
            else:
                st.warning("No consolidated data available after filtering.") # Vertaald
        else:
            st.info("Could not load consolidated data for the selected aggregation.") # Vertaald


    elif not data_df.empty:
        # --- INDIVIDUAL LINE LOGIC ---
        data_df_raw = data_df[
            (data_df[time_column].dt.date >= start_date) & 
            (data_df[time_column].dt.date <= end_date)
        ].copy()

        if data_df_raw.empty:
            st.warning("No data available for the selected date range. Please adjust the start and end dates.")
        else:
            all_sensor_cols = data_df_raw.columns.drop(time_column)
            
            if not all_sensor_cols.empty:
                
                # 4. Sensor Multi-Select 
                selected_sensors = st.sidebar.multiselect(
                    'Select Sensors to Display (Graphs):',
                    options=all_sensor_cols.tolist(),
                    default=all_sensor_cols.tolist(),
                    key='line_sensor_select'
                )
                
                # Filter the DataFrame based on the user selection
                sensor_cols_filtered = [col for col in selected_sensors if col in all_sensor_cols]
                data_df_filtered = data_df_raw[[time_column] + sensor_cols_filtered].copy()
                
                # --- DATA PREPARATION ---
                
                # 1. Calculate the Consumption data (Integration)
                data_df_consumption = calculate_consumption(data_df_filtered, selected_aggregation_display, time_column, sensor_cols_filtered)
                
                # 2. Calculate the Averaged Trend data (Smoothing)
                data_df_trend = data_df_filtered.copy()
                if smoothing_window > 1 and not is_yearly:
                    for col in sensor_cols_filtered:
                        data_df_trend[col] = data_df_trend[col].rolling(window=smoothing_window, min_periods=1).mean()
                
                # --- TAB NAVIGATION ---
                tab_trend, tab_consumption = st.tabs(["📊 Average Output (Intensity)", "📈 Total Consumption (Volume)"])

                # Tab 1: Average Output
                with tab_trend:
                    st.markdown(f"## Average Output: {selected_sidgal_display} ({selected_aggregation_display})")
                    plot_data(data_df_trend, time_column, sensor_cols_filtered, selected_aggregation_display, selected_sidgal_display, smoothing_window, is_consumption=False)

                # Tab 2: Total Consumption
                with tab_consumption:
                    st.markdown(f"## Total Consumption: {selected_sidgal_display} ({selected_aggregation_display})")
                    plot_data(data_df_consumption, time_column, sensor_cols_filtered, selected_aggregation_display, selected_sidgal_display, smoothing_window=1, is_consumption=True)
                
            else:
                st.warning("No sensor data columns found in the selected sheet or no sensors were selected.")
                
    else:
        if not is_site_total:
             st.info("Data could not be loaded for the selected SIDGAL Line. Please check the sheet names.")


# Hoofdtab 2: Data Quick-Check
with tab_quick_check:
    st.markdown(f"## 🔍 Data Quick-Check ({selected_aggregation_display})")
    
    # Determine which DataFrame to use (Site Total or Individual Line)
    df_for_quick_check = site_data_all_time if is_site_total and 'site_data_all_time' in locals() else data_df

    if df_for_quick_check.empty:
        st.info("Cannot load data for the Quick-Check. Please select a SIDGAL Line and Aggregation.") # Vertaald
    else:
        # Field for selecting a specific date/time
        time_col = df_for_quick_check.columns[0]
        max_date_check = df_for_quick_check[time_col].max().date()
        min_date_check = df_for_quick_check[time_col].min().date()

        col_date, col_hour = st.columns([1, 1])
        with col_date:
            selected_date = st.date_input('Choose a date:', max_date_check, min_value=min_date_check, max_value=max_date_check) # Vertaald

        selected_dt = pd.to_datetime(selected_date)

        # For 'Hourly' aggregation, add an hour
        if selected_aggregation_display == 'Hourly':
            with col_hour:
                 selected_time = st.time_input('Choose a time (start hour):', time(0, 0), step=3600) # Vertaald
            selected_dt = selected_dt.replace(hour=selected_time.hour)
            # Find the exact match for the hour (start of the interval)
            df_result = df_for_quick_check[df_for_quick_check[time_col] == selected_dt].copy()
            st.markdown(f"**Search result for:** {selected_dt.strftime('%Y-%m-%d %H:%M:%S')}") # Vertaald
        else:
            st.markdown(f"**Search result for:** {selected_dt.strftime('%Y-%m-%d')} ({selected_aggregation_display} Period)") # Vertaald
            # Find the exact match for the day/week/month/year (start of the interval)
            df_result = df_for_quick_check[df_for_quick_check[time_col].dt.date == selected_dt.date()].copy()
            if df_result.empty and selected_aggregation_display != 'Daily':
                # E.g., search for the start of the selected week/month/year
                st.info("No exact match found. The display shows data based on the closest date if possible.") # Vertaald
                df_result = df_for_quick_check[df_for_quick_check[time_col].dt.date >= selected_dt.date()].head(1).copy()
            
        
        if df_result.empty:
            st.warning("No data found for the selected date and aggregation. Please try a different date.") # Vertaald
        else:
            # Transpose and format the data for a better display
            df_display = df_result.drop(columns=[time_col]).T.rename(columns={df_result.index[0]: 'Average Output'})
            
            # Add units and format the values
            df_display['Unit'] = [get_sensor_unit(sensor) for sensor in df_display.index]
            df_display['Average Output'] = df_display['Average Output'].apply(lambda x: f"{x:,.2f}").str.replace(',', ' ').str.replace('.', ',')

            # Reorder columns
            df_display = df_display[['Average Output', 'Unit']]

            st.dataframe(df_display, use_container_width=True)