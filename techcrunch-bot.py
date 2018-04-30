
# load libraries
import re
import requests                  as rq
import pandas                    as pd

from bs4    import BeautifulSoup as bs
from sqlalchemy import create_engine

# ---------------------------------
# scrape informations
# ---------------------------------
url = 'https://techcrunch.com'
html = rq.get( url )

# beautifulsoup
soup = bs( html.content, 'html.parser' )

# scraping informations
# titles
titles = soup.find_all( 'h2', class_='post-block__title' )
titles = [title.get_text(strip=True) for title in titles] 

# publish date
times = soup.find_all( 'time', class_='river-byline__time' )
times = [time['datetime'] for time in times]

# descriptions
descriptions = soup.find_all( 'div', class_='post-block__content' )
descriptions = [description.get_text(strip=True) for description in descriptions]

# links
links = soup.find_all( 'a', class_='post-block__title__link' )
links = [link['href'] for link in links]

# tags
#cdata = soup.find( text=re.compile( 'CDATA' ) )

# content
contents_list = []
authors_list = []
for link in links:
    html = rq.get( link )
    soup = bs( html.content, 'html.parser' )

    # contents
    contents = soup.find_all( 'p' )
    contents = [content.get_text(strip=True) for content in contents]
    contents_list.append( contents )

    # authors
    authors = soup.find_all( 'div', class_='article__byline' )
    authors = [author.find('a').get_text(strip=True) for author in authors] 
    authors_list.append( authors )

contents_list = ['<p>'.join( content ) for content in contents_list]
authors_list = [','.join(author) for author in authors_list]
cols = ['title', 'author', 'publish_date', 'description', 'link', 'content']
df = pd.DataFrame( {'title': titles, 'author': authors_list, 'publish_date': times, 'description': descriptions, 'link':links, 'content': contents_list}, columns = cols )

# save to .csv file
#df.to_csv( 'result-techcrunch.csv', index=False, header=cols, sep=';' )

# query current postgresql database for max post date
engine = 'postgresql://meigarom@localhost/postgres'
conn = create_engine( engine, client_encoding='utf-8' )
query = 'select max( publish_date ) as max_publish_date from public.techcrunch_posts'
df1 = pd.read_sql( query, con=conn )
max_publish_date = df1['max_publish_date'][0]

# Case table is empty
if max_publish_date == None:
    print( 'empty table' )
    max_publish_date = '1990-01-01T00:00:00+00:00'
else:
    max_publish_date = df1['max_publish_date'][0].strftime( '%Y-%m-%dT%H:%M:%S+00:00' )
    
# Check if there is a new post
a = df['publish_date'][0] > max_publish_date

if a == True:
    print( '\nthere is new post' )
    print( links )
    df2 = df[df['publish_date'] > max_publish_date]
    df2.to_sql('techcrunch_posts', schema='public', con=conn, if_exists='append', index=False)
else:
    print( '\nthere is no new post' )

print( '\nlast post published on techcrunch: %s' % df['publish_date'][0] )
print( 'last post in the database: %s' %  max_publish_date )
