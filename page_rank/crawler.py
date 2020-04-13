import psycopg2
import requests
from nltk.corpus import stopwords
from bs4 import BeautifulSoup


def get_connection():
    connection = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="mbr2010IL",
        host="database-3-instance-1.c0sw2mz3atbl.us-east-1.rds.amazonaws.com",
        port='5432'
    )
    return connection


def get_domain(ref):
    count = 3
    result = ''
    for char in ref:
        if count == 0:
            break
        if char == '/':
            count = count - 1
        result = result + char
    if not result.endswith('/'):
        result += '/'
    return result


def get_inside_links(input_ref):
    domain = get_domain(input_ref)
    response = requests.get(input_ref)
    soup = BeautifulSoup(response.text)
    hrefs = []
    for a in soup.find_all(u'a'):
        print(a)
        try:
            if a.attrs:
                hrefs.append(a.attrs['href'])
        except Exception as e:
            print(e)
    links = list(filter(
        lambda ref:
        ref.startswith(domain) or
        ref.startswith('/'),
        hrefs))
    for i in range(0, len(links)):
        if links[i].startswith('/'):
            links[i] = domain + links[i][1:]
    return links


def insert_links(links, connection, visited_links, source_link):
    cursor = connection.cursor()
    for link in links:
        if link not in visited_links:
            try:
                cursor.execute("""INSERT INTO pages (page_id) VALUES('{0}')""".format(link))
                connection.commit()
                cursor.execute(
                    """INSERT INTO transition (source_id, destination_id) VALUES('{0}', '{1}')"""
                    .format(source_link.replace("'", ""), link.replace("'", "")))
                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()
            visited_links.append(link)
            parse_words(link, connection)
            crawling(link, visited_links)
        else:
            try:
                cursor.execute(
                    """INSERT INTO transition (source_id, destination_id) VALUES('{0}', '{1}')"""
                    .format(source_link.replace("'", ""), link.replace("'", "")))
                connection.commit()
            except Exception as e:
                print(e)
                connection.rollback()


def crawling(ref, visited_links):
    connection = get_connection()
    links = get_inside_links(ref)
    insert_links(links, connection, visited_links, ref)


def parse_words(link, connection):
    response = requests.get(link)
    html = response.text
    cursor = connection.cursor()
    cursor.execute("""SELECT * FROM pages WHERE page_id = '{0}'""".format(link))
    page_id = [row[2] for row in cursor.fetchall()][0]
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
                    try:
                        cursor.execute("""UPDATE words SET refs = '{0}' WHERE words = '{1}';"""
                                       .format(refs_existing_words[word].replace("'", ""), word.replace("'", "")))
                        connection.commit()
                    except Exception as e:
                        print(e)
                        connection.rollback()
            else:
                try:
                    cursor.execute("""INSERT INTO words (words, refs) VALUES('{0}', '{1}')"""
                                   .format(word.replace("'", ""), str(page_id).replace("'", "")))
                    connection.commit()
                    existing_words.append(word)
                    refs_existing_words[word] = str(page_id)
                except Exception as e:
                    print(e)
                    connection.rollback()
    print('%l has been parsed', link)


def main():
    visited_links = []
    domain = 'https://www.postgresqltutorial.com'
    crawling(domain, visited_links)
    print('Hello')


if __name__ == '__main__':
    main()
