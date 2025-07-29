import pandas as pd
import matplotlib.pyplot as plt


url = "https://raw.githubusercontent.com/geethasamynathan/Hexa_22July/main/Python/Demos/Pandas/winemag-data-130k-v2.csv"
df = pd.read_csv(url, index_col=0)


df = df.dropna(subset=['country', 'points'])

#Group by country and calculate average points
avg_points = df.groupby('country')['points'].mean()

# Step 4: Sort the result in descending order and get top 10 countries
top10_countries = avg_points.sort_values(ascending=False).head(10)

# Step 5: Plot horizontal bar chart
plt.figure(figsize=(10, 6))
top10_countries.sort_values().plot(kind='barh', color='orchid')  # sort again to get lowest at top
plt.xlabel('Average Wine Rating (Points)')
plt.ylabel('Country')
plt.title('Top 10 Countries by Average Wine Rating')
plt.tight_layout()
plt.show()
