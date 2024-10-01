from utils.db_utils import *
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# Configura o layout da página para Wide Mode
st.set_page_config(layout="wide")

def dashboard_partner_page():
    st.title('Análise de Pedidos por Franquia')

    # Fetch the data using the query
    df = fetch_data_sl_clients_performance()

    # Get unique values for partners and months
    partners = df['partner_nome'].unique()
    months = df['month'].unique()

    # Add partner filter
    selected_partner = st.selectbox('Selecione um parceiro (ou todos):', ['Todos'] + list(partners))
    
    # Add month filter
    selected_month = st.selectbox('Selecione um mês (ou todos):', ['Todos'] + list(months))

    # Filter data by selected partner and month
    if selected_partner != 'Todos':
        df = df[df['partner_nome'] == selected_partner]

    if selected_month != 'Todos':
        df = df[df['month'] == selected_month]

    # Display pie charts based on the filtered data
    charts = plot_pie_chart(df)

    # Display charts in a multi-column layout
    num_cols = 4
    cols = st.columns(num_cols)
    for i, chart in enumerate(charts):
        with cols[i % num_cols]:
            st.plotly_chart(chart, use_container_width=True)

# Function to plot pie charts using Plotly
def plot_pie_chart(df):
    charts = []
    
    for index, row in df.iterrows():
        values = [row['orders_ok'], row['total_clients'] - row['orders_ok']]
        labels = ['OK', 'Pendentes']
        colors = ['#28a745', '#dc3545']  # Green for OK, Red for Pending

        # Check if there are values to plot
        if sum(values) > 0:
            # Create the pie chart with Plotly
            fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.3, 
                                         textinfo='label+percent', 
                                         marker=dict(colors=colors))])
            
            # Update the title to include partner name and total clients
            fig.update_layout(title_text=f'{row["partner_nome"]} ({row["total_clients"]}) - Mês {row["month"]}', showlegend=False)

            # Append chart to the list
            charts.append(fig)
    
    return charts

# Load the dashboard
dashboard_partner_page()
