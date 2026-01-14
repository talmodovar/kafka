#!/usr/bin/env python3
"""
Exercise 8: Weather Data Visualizations
Creates visualizations from aggregated weather data.
"""

import os
import glob
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# Configuration
INPUT_PATH = "./output"
OUTPUT_PATH = "./visualizations"

def load_csv_from_spark_output(path):
    """
    Load CSV from Spark output directory (handles multiple part files).
    
    Args:
        path: Path to Spark output directory
    
    Returns:
        pandas DataFrame
    """
    csv_files = glob.glob(f"{path}/part-*.csv")
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {path}")
    
    # Read all part files and concatenate
    dfs = [pd.read_csv(f) for f in csv_files]
    return pd.concat(dfs, ignore_index=True)


def create_output_directory():
    """Create output directory for visualizations."""
    Path(OUTPUT_PATH).mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {OUTPUT_PATH}")


def plot_temperature_evolution():
    """
    Visualization 1: Temperature evolution over time by city.
    """
    print("\n" + "=" * 60)
    print("Creating Temperature Evolution Chart...")
    print("=" * 60)
    
    try:
        df = load_csv_from_spark_output(f"{INPUT_PATH}/temperature_evolution")
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        # Create plot
        plt.figure(figsize=(14, 7))
        
        # Plot each city
        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            plt.plot(city_data['timestamp'], city_data['temperature'], 
                    marker='o', label=f"{city}, {city_data['country'].iloc[0]}", 
                    linewidth=2, markersize=4)
        
        plt.xlabel('Time', fontsize=12, fontweight='bold')
        plt.ylabel('Temperature (°C)', fontsize=12, fontweight='bold')
        plt.title('Temperature Evolution Over Time by City', fontsize=14, fontweight='bold')
        plt.legend(loc='best')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        output_file = f"{OUTPUT_PATH}/temperature_evolution.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()
        
    except Exception as e:
        print(f"✗ Error creating temperature evolution chart: {e}")


def plot_wind_evolution():
    """
    Visualization 2: Wind speed evolution over time by city.
    """
    print("\n" + "=" * 60)
    print("Creating Wind Speed Evolution Chart...")
    print("=" * 60)
    
    try:
        df = load_csv_from_spark_output(f"{INPUT_PATH}/wind_evolution")
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        # Create plot
        plt.figure(figsize=(14, 7))
        
        # Plot each city
        for city in df['city'].unique():
            city_data = df[df['city'] == city]
            plt.plot(city_data['timestamp'], city_data['windspeed'], 
                    marker='s', label=f"{city}, {city_data['country'].iloc[0]}", 
                    linewidth=2, markersize=4)
        
        plt.xlabel('Time', fontsize=12, fontweight='bold')
        plt.ylabel('Wind Speed (m/s)', fontsize=12, fontweight='bold')
        plt.title('Wind Speed Evolution Over Time by City', fontsize=14, fontweight='bold')
        plt.legend(loc='best')
        plt.xticks(rotation=45)
        plt.tight_layout()
        
        output_file = f"{OUTPUT_PATH}/wind_evolution.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()
        
    except Exception as e:
        print(f"✗ Error creating wind evolution chart: {e}")


def plot_alert_distribution():
    """
    Visualization 3: Alert counts by type and level.
    """
    print("\n" + "=" * 60)
    print("Creating Alert Distribution Chart...")
    print("=" * 60)
    
    try:
        df = load_csv_from_spark_output(f"{INPUT_PATH}/alert_counts")
        
        # Pivot data for stacked bar chart
        pivot_df = df.pivot(index='alert_type', columns='alert_level', values='count').fillna(0)
        
        # Create plot
        fig, ax = plt.subplots(figsize=(10, 6))
        
        pivot_df.plot(kind='bar', stacked=True, ax=ax, 
                     color=['#2ecc71', '#f39c12', '#e74c3c'])
        
        ax.set_xlabel('Alert Type', fontsize=12, fontweight='bold')
        ax.set_ylabel('Count', fontsize=12, fontweight='bold')
        ax.set_title('Alert Distribution by Type and Level', fontsize=14, fontweight='bold')
        ax.legend(title='Alert Level', labels=['Level 0', 'Level 1', 'Level 2'])
        ax.set_xticklabels(ax.get_xticklabels(), rotation=0)
        plt.tight_layout()
        
        output_file = f"{OUTPUT_PATH}/alert_distribution.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()
        
    except Exception as e:
        print(f"✗ Error creating alert distribution chart: {e}")


def plot_weather_codes():
    """
    Visualization 4: Most frequent weather code by country.
    """
    print("\n" + "=" * 60)
    print("Creating Weather Code Frequency Chart...")
    print("=" * 60)
    
    try:
        df = load_csv_from_spark_output(f"{INPUT_PATH}/weather_codes_by_country")
        
        # Create plot
        plt.figure(figsize=(12, 6))
        
        bars = plt.bar(df['country'], df['count'], color='steelblue', edgecolor='black')
        
        # Add weather code labels on bars
        for i, (country, code, count) in enumerate(zip(df['country'], df['weather_code'], df['count'])):
            plt.text(i, count + max(df['count']) * 0.02, f"Code: {int(code)}", 
                    ha='center', va='bottom', fontweight='bold')
        
        plt.xlabel('Country', fontsize=12, fontweight='bold')
        plt.ylabel('Frequency', fontsize=12, fontweight='bold')
        plt.title('Most Frequent Weather Code by Country', fontsize=14, fontweight='bold')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        
        output_file = f"{OUTPUT_PATH}/weather_codes_by_country.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()
        
    except Exception as e:
        print(f"✗ Error creating weather code chart: {e}")


def plot_summary_statistics():
    """
    Bonus: Summary statistics visualization.
    """
    print("\n" + "=" * 60)
    print("Creating Summary Statistics Chart...")
    print("=" * 60)
    
    try:
        df = load_csv_from_spark_output(f"{INPUT_PATH}/summary_statistics")
        
        # Create subplots
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))
        
        # 1. Total alerts by city
        ax1 = axes[0, 0]
        df_sorted = df.sort_values('total_alerts', ascending=False).head(10)
        ax1.barh(df_sorted['city'], df_sorted['total_alerts'], color='coral')
        ax1.set_xlabel('Total Alerts', fontweight='bold')
        ax1.set_ylabel('City', fontweight='bold')
        ax1.set_title('Top 10 Cities by Alert Count', fontweight='bold')
        ax1.invert_yaxis()
        
        # 2. Average temperature by city
        ax2 = axes[0, 1]
        df_sorted = df.sort_values('avg_temperature', ascending=False).head(10)
        ax2.barh(df_sorted['city'], df_sorted['avg_temperature'], color='orange')
        ax2.set_xlabel('Average Temperature (°C)', fontweight='bold')
        ax2.set_ylabel('City', fontweight='bold')
        ax2.set_title('Top 10 Cities by Average Temperature', fontweight='bold')
        ax2.invert_yaxis()
        
        # 3. Max windspeed by city
        ax3 = axes[1, 0]
        df_sorted = df.sort_values('max_windspeed', ascending=False).head(10)
        ax3.barh(df_sorted['city'], df_sorted['max_windspeed'], color='skyblue')
        ax3.set_xlabel('Max Wind Speed (m/s)', fontweight='bold')
        ax3.set_ylabel('City', fontweight='bold')
        ax3.set_title('Top 10 Cities by Max Wind Speed', fontweight='bold')
        ax3.invert_yaxis()
        
        # 4. Temperature range by city
        ax4 = axes[1, 1]
        df['temp_range'] = df['max_temperature'] - df['min_temperature']
        df_sorted = df.sort_values('temp_range', ascending=False).head(10)
        ax4.barh(df_sorted['city'], df_sorted['temp_range'], color='lightgreen')
        ax4.set_xlabel('Temperature Range (°C)', fontweight='bold')
        ax4.set_ylabel('City', fontweight='bold')
        ax4.set_title('Top 10 Cities by Temperature Range', fontweight='bold')
        ax4.invert_yaxis()
        
        plt.tight_layout()
        
        output_file = f"{OUTPUT_PATH}/summary_statistics.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✓ Saved: {output_file}")
        plt.close()
        
    except Exception as e:
        print(f"✗ Error creating summary statistics chart: {e}")


def main():
    """Main function to create all visualizations."""
    print("=" * 60)
    print("Weather Data Visualization - Exercise 8")
    print("=" * 60)
    
    # Create output directory
    create_output_directory()
    
    # Create all visualizations
    plot_temperature_evolution()
    plot_wind_evolution()
    plot_alert_distribution()
    plot_weather_codes()
    plot_summary_statistics()
    
    print("\n" + "=" * 60)
    print("All visualizations created successfully!")
    print(f"Check the '{OUTPUT_PATH}' directory for output files.")
    print("=" * 60)


if __name__ == "__main__":
    main()
