import itertools
import psycopg2
from nltk.corpus import stopwords
from django.shortcuts import render
from .forms import SearchForm


def home(request):
    links = []
    if request.method == 'POST':
        form = SearchForm(request.POST)
        if form.is_valid():
            cd = form.cleaned_data
            links = get_links(cd['search_input'])

    return render(request, 'home.html', {'links': links})


def get_links(search_text):
    result_list = []
    my_stopwords = list(set(stopwords.words('english'))) + list(set(stopwords.words('russian')))
    search_text = list(set([word for word in search_text.split() if word not in my_stopwords]))
    words_refs = get_refs(search_text)
    combinations = []
    for i in range(1, len(search_text) + 1):
        combinations += itertools.combinations(search_text, i)
    for pair in combinations:
        inter = words_refs[pair[0]]
        for i in range(1, len(pair)):
            inter = intersection(inter, words_refs[pair[i]])
        result_list += inter
    result_list = list(set(result_list))
    page_ranks = get_page_ranks(result_list)
    return page_ranks


def get_page_ranks(link_ids):
    result_links = []
    connection = get_connection()
    cursor = connection.cursor()
    for _id in link_ids:
        cursor.execute("""SELECT page_id, rank FROM pages WHERE id = {0}""".format(_id))
        result_links.append([row for row in cursor.fetchall()][0])
    result_links = list(map(lambda x: x[0], sorted(result_links, key=lambda x: x[1], reverse=True)))
    return result_links


def intersection(lst1, lst2):
    lst3 = [value for value in lst1 if value in lst2]
    return lst3


def get_refs(words):
    result = {}
    connection = get_connection()
    cursor = connection.cursor()
    for word in words:
        cursor.execute("""SELECT * FROM words WHERE  words = '{0}';""".format(word))
        refs = [row[1] for row in cursor.fetchall()][0].split(',')
        result[word] = refs
    return result


def get_connection():
    connection = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="mbr2010IL",
        host="database-3-instance-1.c0sw2mz3atbl.us-east-1.rds.amazonaws.com",
        port='5432'
    )
    return connection
    pass
