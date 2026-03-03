"""Flask Dashboard for Operations KPIs"""
from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import json
from pathlib import Path
import tempfile
import os
import sys

# Add error handling for imports
try:
    from src.ingest import DataIngester, DatasetLoader
    from src.normalize import normalize_dataframe
except ImportError as e:
    print(f"Import error: {e}")
    print(f"Python path: {sys.path}")
    print(f"Current directory: {os.getcwd()}")
    # Try alternative import
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from src.ingest import DataIngester, DatasetLoader
        from src.normalize import normalize_dataframe
    except ImportError:
        raise

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

# Use /tmp for serverless environments (Vercel)
upload_folder = '/tmp' if os.environ.get('VERCEL') else tempfile.mkdtemp()
app.config['UPLOAD_FOLDER'] = upload_folder

# Ensure upload folder exists
os.makedirs(upload_folder, exist_ok=True)

# Global storage for processed data
processed_data = {}


@app.route('/')
def index():
    """Main dashboard page"""
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file uploads"""
    try:
        files = request.files
        
        required_files = [
            'delivery_details',
            'parcel_logs',
            'collectors_report',
            'prepare_report',
            'items_per_order',
            'freshdesk_data'
        ]
        
        # Validate all files are present
        missing_files = [f for f in required_files if f not in files]
        if missing_files:
            return jsonify({
                'success': False,
                'error': f'Missing files: {", ".join(missing_files)}'
            }), 400
        
        # Save uploaded files
        saved_files = {}
        for file_key in required_files:
            file = files[file_key]
            if file.filename == '':
                return jsonify({
                    'success': False,
                    'error': f'No file selected for {file_key}'
                }), 400
            
            filepath = Path(app.config['UPLOAD_FOLDER']) / file.filename
            file.save(filepath)
            saved_files[file_key] = filepath
        
        # Process files
        ingester = DataIngester()
        loader = DatasetLoader(ingester)
        
        datasets = {}
        datasets['delivery_details'] = loader.load_delivery_details(saved_files['delivery_details'])
        datasets['parcel_logs'] = loader.load_parcel_logs(saved_files['parcel_logs'])
        datasets['collectors_report'] = loader.load_collectors_report(saved_files['collectors_report'])
        datasets['prepare_report'] = loader.load_prepare_report(saved_files['prepare_report'])
        datasets['items_per_order'] = loader.load_items_per_order(saved_files['items_per_order'])
        datasets['freshdesk_data'] = loader.load_freshdesk_data(saved_files['freshdesk_data'])
        
        # Normalize data
        for name, df in datasets.items():
            datasets[name] = normalize_dataframe(df, detect_wa=True)
        
        # Store in global state
        global processed_data
        processed_data = datasets
        
        # Calculate summary stats
        total_parcels = len(datasets['delivery_details'])
        total_delivered = len(datasets['delivery_details'][
            datasets['delivery_details']['order_status'] == 'Delivered'
        ]) if 'order_status' in datasets['delivery_details'].columns else 0
        
        wa_count = datasets['delivery_details']['has_waiting_address'].sum() if 'has_waiting_address' in datasets['delivery_details'].columns else 0
        
        return jsonify({
            'success': True,
            'message': 'Files uploaded and processed successfully',
            'stats': {
                'total_parcels': int(total_parcels),
                'total_delivered': int(total_delivered),
                'waiting_address_count': int(wa_count)
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/dashboard/dod-trend')
def dod_trend():
    """Daily DOD trend chart"""
    if 'delivery_details' not in processed_data:
        return jsonify({'error': 'No data loaded'}), 400
    
    df = processed_data['delivery_details'].copy()
    
    # Parse delivery date
    if 'delivery_date' in df.columns:
        df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')
        df['date'] = df['delivery_date'].dt.date
    else:
        return jsonify({'error': 'delivery_date column not found'}), 400
    
    # Group by date and warehouse
    daily_stats = df.groupby(['date', 'warehouse']).agg({
        'parcel_id': 'count'
    }).reset_index()
    daily_stats.columns = ['date', 'warehouse', 'delivered']
    
    # Create combo chart (bars + line)
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    for warehouse in daily_stats['warehouse'].unique():
        wh_data = daily_stats[daily_stats['warehouse'] == warehouse]
        
        # Add bar trace for delivered count
        fig.add_trace(
            go.Bar(
                x=wh_data['date'],
                y=wh_data['delivered'],
                name=f'{warehouse} - Delivered',
                text=wh_data['delivered'],
                textposition='auto'
            ),
            secondary_y=False
        )
    
    fig.update_layout(
        title='Daily Delivered Parcels Trend',
        xaxis_title='Date',
        yaxis_title='Delivered Count',
        barmode='group',
        height=500
    )
    
    return jsonify(json.loads(fig.to_json()))


@app.route('/dashboard/warehouse-comparison')
def warehouse_comparison():
    """On-time % by warehouse comparison"""
    if 'delivery_details' not in processed_data:
        return jsonify({'error': 'No data loaded'}), 400
    
    df = processed_data['delivery_details'].copy()
    
    # Group by warehouse
    warehouse_stats = df.groupby('warehouse').agg({
        'parcel_id': 'count'
    }).reset_index()
    warehouse_stats.columns = ['warehouse', 'total_parcels']
    
    # Create bar chart
    fig = go.Figure(data=[
        go.Bar(
            x=warehouse_stats['warehouse'],
            y=warehouse_stats['total_parcels'],
            text=warehouse_stats['total_parcels'],
            textposition='auto',
            marker_color='lightblue'
        )
    ])
    
    fig.update_layout(
        title='Parcels by Warehouse',
        xaxis_title='Warehouse',
        yaxis_title='Total Parcels',
        height=400
    )
    
    return jsonify(json.loads(fig.to_json()))


@app.route('/dashboard/waiting-address')
def waiting_address_analysis():
    """Waiting Address (WA) analysis"""
    if 'delivery_details' not in processed_data:
        return jsonify({'error': 'No data loaded'}), 400
    
    df = processed_data['delivery_details'].copy()
    
    if 'has_waiting_address' not in df.columns:
        return jsonify({'error': 'Waiting Address detection not available'}), 400
    
    # Group by warehouse and WA status
    wa_stats = df.groupby(['warehouse', 'has_waiting_address']).size().reset_index(name='count')
    
    # Create stacked bar chart
    fig = go.Figure()
    
    for wa_status in [True, False]:
        data = wa_stats[wa_stats['has_waiting_address'] == wa_status]
        fig.add_trace(go.Bar(
            x=data['warehouse'],
            y=data['count'],
            name='Waiting Address' if wa_status else 'Normal Address',
            text=data['count'],
            textposition='auto'
        ))
    
    fig.update_layout(
        title='Waiting Address (WA) Analysis by Warehouse',
        xaxis_title='Warehouse',
        yaxis_title='Parcel Count',
        barmode='stack',
        height=400
    )
    
    return jsonify(json.loads(fig.to_json()))


@app.route('/dashboard/area-breakdown')
def area_breakdown():
    """SLA breakdown by area"""
    if 'delivery_details' not in processed_data:
        return jsonify({'error': 'No data loaded'}), 400
    
    df = processed_data['delivery_details'].copy()
    
    # Group by area
    if 'area' in df.columns:
        area_stats = df.groupby('area').size().reset_index(name='count')
        area_stats = area_stats.sort_values('count', ascending=False).head(20)
        
        fig = go.Figure(data=[
            go.Bar(
                x=area_stats['area'],
                y=area_stats['count'],
                text=area_stats['count'],
                textposition='auto'
            )
        ])
        
        fig.update_layout(
            title='Top 20 Areas by Parcel Volume',
            xaxis_title='Area',
            yaxis_title='Parcel Count',
            height=500
        )
        
        return jsonify(json.loads(fig.to_json()))
    else:
        return jsonify({'error': 'area column not found'}), 400


@app.route('/dashboard/staff-productivity')
def staff_productivity():
    """Staff productivity metrics"""
    charts = []
    
    # Collectors productivity
    if 'collectors_report' in processed_data:
        df = processed_data['collectors_report'].copy()
        if 'collector' in df.columns:
            collector_stats = df.groupby('collector').agg({
                'parcel_id': 'count',
                'duration': 'mean'
            }).reset_index()
            collector_stats.columns = ['collector', 'parcels', 'avg_duration']
            collector_stats = collector_stats.sort_values('parcels', ascending=False).head(10)
            
            fig = go.Figure(data=[
                go.Bar(
                    x=collector_stats['collector'],
                    y=collector_stats['parcels'],
                    text=collector_stats['parcels'],
                    textposition='auto',
                    name='Parcels Collected'
                )
            ])
            
            fig.update_layout(
                title='Top 10 Collectors by Volume',
                xaxis_title='Collector',
                yaxis_title='Parcels Collected',
                height=400
            )
            
            charts.append({'type': 'collectors', 'chart': json.loads(fig.to_json())})
    
    # Preparers productivity
    if 'prepare_report' in processed_data:
        df = processed_data['prepare_report'].copy()
        if 'preparer' in df.columns:
            preparer_stats = df.groupby('preparer').agg({
                'parcel_id': 'count',
                'duration': 'mean'
            }).reset_index()
            preparer_stats.columns = ['preparer', 'parcels', 'avg_duration']
            preparer_stats = preparer_stats.sort_values('parcels', ascending=False).head(10)
            
            fig = go.Figure(data=[
                go.Bar(
                    x=preparer_stats['preparer'],
                    y=preparer_stats['parcels'],
                    text=preparer_stats['parcels'],
                    textposition='auto',
                    name='Parcels Prepared',
                    marker_color='lightgreen'
                )
            ])
            
            fig.update_layout(
                title='Top 10 Preparers by Volume',
                xaxis_title='Preparer',
                yaxis_title='Parcels Prepared',
                height=400
            )
            
            charts.append({'type': 'preparers', 'chart': json.loads(fig.to_json())})
    
    return jsonify({'charts': charts})


@app.route('/dashboard/dod-summary-table')
def dod_summary_table():
    """DOD Summary Table with daily metrics"""
    if 'delivery_details' not in processed_data:
        return jsonify({'error': 'No data loaded'}), 400
    
    df = processed_data['delivery_details'].copy()
    
    # Parse delivery date
    if 'delivery_date' in df.columns:
        df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')
        df['date'] = df['delivery_date'].dt.date
    else:
        return jsonify({'error': 'delivery_date column not found'}), 400
    
    # Calculate on-time status (simplified - you'd need SLA logic for real calculation)
    # For now, assume we have an 'sla_status' column or calculate based on available data
    df['is_on_time'] = True  # Placeholder - replace with actual SLA calculation
    df['is_late'] = False
    
    # Group by date
    summary = df.groupby('date').agg({
        'parcel_id': 'count',
        'is_on_time': 'sum',
        'is_late': 'sum'
    }).reset_index()
    
    summary.columns = ['Date', 'Total Orders', 'On Time', 'Late']
    summary['On Time %'] = ((summary['On Time'] / summary['Total Orders']) * 100).round(0).astype(int).astype(str) + '%'
    
    # Convert to dict for JSON response
    return jsonify({
        'columns': ['Date', 'Total Orders', 'On Time', 'Late', 'On Time %'],
        'data': summary.to_dict('records')
    })


@app.route('/dashboard/waiting-address-table')
def waiting_address_table():
    """Waiting Address breakdown table by date"""
    if 'delivery_details' not in processed_data:
        return jsonify({'error': 'No data loaded'}), 400
    
    df = processed_data['delivery_details'].copy()
    
    if 'has_waiting_address' not in df.columns:
        return jsonify({'error': 'Waiting Address detection not available'}), 400
    
    # Parse delivery date
    if 'delivery_date' in df.columns:
        df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')
        df['date'] = df['delivery_date'].dt.date
    else:
        return jsonify({'error': 'delivery_date column not found'}), 400
    
    # Group by date and WA status
    wa_summary = df.groupby(['date', 'has_waiting_address']).size().unstack(fill_value=0).reset_index()
    wa_summary.columns = ['Date', 'Normal', 'Waiting Address']
    
    # Calculate impact percentage
    wa_summary['WA Impact'] = ((wa_summary['Waiting Address'] / (wa_summary['Normal'] + wa_summary['Waiting Address'])) * 100).round(0).astype(int).astype(str) + '%'
    
    return jsonify({
        'columns': ['Date', 'Waiting Address Count', 'WA Impact'],
        'data': wa_summary[['Date', 'Waiting Address', 'WA Impact']].to_dict('records')
    })


@app.route('/export/<dataset>')
def export_csv(dataset):
    """Export dataset as CSV"""
    if dataset not in processed_data:
        return jsonify({'error': 'Dataset not found'}), 404
    
    df = processed_data[dataset]
    
    # Create temporary CSV file
    temp_file = Path(app.config['UPLOAD_FOLDER']) / f'{dataset}_export.csv'
    df.to_csv(temp_file, index=False)
    
    return send_file(temp_file, as_attachment=True, download_name=f'{dataset}.csv')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
