import requests
from nltk.corpus import stopwords
from bs4 import BeautifulSoup


def parse_words(link, connection):
    html = requests.get(link).text
    cursor = connection.cursor()
    cursor.execute("""SELECT * FROM pages WHERE page_id = '{0}'""".format(link))
    page_id = cursor.fetchall()[0][2]
    cursor.execute("""SELECT * FROM words""")
    rows = cursor.fetchall()
    existing_words = [row[0] for row in rows]
    refs_existing_words = {row[0]: row[1] for row in rows}
    soup = BeautifulSoup(html)
    content = soup.text.split()
    symbols = '1 2 3 4 5 6 7 8 9 0 - = + _ ) ( * & ^ % $ # @ ! ± § ` ~ [ ] { } ; : \' " \\ | / ? > . < ,"'.split()
    stop_words = list(set(stopwords.words('russian'))) + list(set(stopwords.words('english'))) + symbols
    for word in content:
        if word not in stop_words:
            if word in existing_words:
                if id not in refs_existing_words[word].split(','):
                    refs_existing_words[word] = refs_existing_words[word] + ',' + str(page_id)
                    cursor.execute("""UPDATE words SET refs = '{0}' WHERE words = '{1}';"""
                                   .format(refs_existing_words[word], word))
                    connection.commit()
            else:
                cursor.execute("""INSERT INTO words (words, refs) VALUES('{0}', '{1}')"""
                               .format(word, str(page_id)))
                connection.commit()
                existing_words.append(word)
                refs_existing_words[word] = str(page_id)
    print('%l has been parsed', link)


# def main():
#     connection = psycopg2.connect(
#         database="postgres",
#         user="postgres",
#         password="mbr2010IL",
#         host="database-3-instance-1.c0sw2mz3atbl.us-east-1.rds.amazonaws.com",
#         port='5432'
#     )
#     domain = 'https://djbook.ru/rel3.0/'
#     response = requests.get(domain)
#     parse(response.text, 1, connection)
#
#
# if __name__ == '__main__':
#     main()
