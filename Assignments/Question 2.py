import pandas as pd
import matplotlib.pyplot as plt

# Step 1: Load the dataset
url = "https://raw.githubusercontent.com/geethasamynathan/Hexa_22July/main/Python/Demos/Pandas/winemag-data-130k-v2.csv"
df = pd.read_csv(url, index_col=0)

# Step 2: Drop rows where 'country' or 'points' is missing
df = df.dropna(subset=['country', 'points'])

# Step 3: Group by country and calculate average rating
avg_ratings = df.groupby('country')['points'].mean()

# Step 4: Sort in descending order and select top 10
top10 = avg_ratings.sort_values(ascending=False).head(10)

# Step 5: Create horizontal bar chart
plt.figure(figsize=(10, 6))
top10.sort_values().plot(kind='barh', color='orchid')  # Sort again for ascending bar display

# Step 6: Customize chart
plt.xlabel('Average Rating (Points)')
plt.ylabel('Country')
plt.title('Top 10 Countries by Average Wine Rating')

# Step 7: Layout adjustment for clarity
plt.xticks(rotation=0)  # Keep x-axis labels straight
plt.tight_layout()      # Adjust layout to prevent label cutoff
plt.grid(axis='x', linestyle='--', alpha=0.7)  # Optional: light grid for readability

# Step 8: Show the chart
plt.show()
