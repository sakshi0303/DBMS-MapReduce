#!pip3 install matplotlib
import matplotlib.pyplot as plt
data = {}
with open('/home/sx3702/Downloads/part-r-00000', 'r') as f:
    for line in f:
        year_genre, count = line.strip().split('\t')
        year, genre = year_genre.split(' | ')
        count = int(count)
        if year not in data:
            data[year] = {}
        data[year][genre] = count
print(data)
# data = {
#     '2000-2006': {'Action/Drama': 167371, 'Adventure/Sci-Fi': 11891, 'Comedy/Romance': 262975},
#     '2007-2013': {'Action/Drama': 330421, 'Adventure/Sci-Fi': 27804, 'Comedy/Romance': 496074},
#     '2014-2020': {'Action/Drama': 474792, 'Adventure/Sci-Fi': 49363, 'Comedy/Romance': 708296}
# }
def plot_histogram(start_date, end_date, genres):
    #genres = ['Action/Drama', 'Adventure/Sci-Fi', 'Comedy/Romance']
    data_range = {}
    for year in data:
        if start_date <= year <= end_date:
            for genre in genres:
                if genre in data[year]:
                    if year in data_range:
                        data_range[year][genre] = data[year][genre]
                    else:
                        data_range[year] = {genre: data[year][genre]}    
    fig, ax = plt.subplots()
    for genre in genres:
        genre_data = [data_range[year][genre] for year in data_range]
        ax.bar(data_range.keys(), genre_data, label=genre)
    ax.set_xlabel('Year Range')
    ax.set_ylabel('Number of Movies')
    ax.set_title('Movie Genres by Year Range')
    ax.legend()
    plt.show()
plot_histogram('2000-2006', '2014-2020', ['Action/Drama'])
plot_histogram('2000-2006', '2014-2020', ['Adventure/Sci-Fi'])
plot_histogram('2000-2006', '2014-2020', ['Comedy/Romance'])