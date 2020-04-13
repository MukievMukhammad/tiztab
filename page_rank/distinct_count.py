import psycopg2


def get_binared(items):
    new = []
    for i in items:
        new.append(format(i, "b"))

    return new


def h1(items):
    #   (2x + 1) mod 32
    hashes = []
    for i in items:
        new_hash = (i * 2 + 1) % 32
        hashes.append(new_hash)
    return hashes


def count_zeros_in_bin_num_rule(items):
    l = []
    for i in items:
        if '1' in i:
            l.append(len(i) - i.rfind('1') - 1)
        else:
            l.append(0)
    return l



def main():
    conn = psycopg2.connect(
        database="test",
        user="postgres",
        password="0110",
        host="localhost",
        port='5432'
    )

    cur = conn.cursor()
    cur.execute("""select num from t4""")
    rows = [r[0] for r in cur.fetchall()]
    hashed_rows = h1(rows)
    binary_hashed_rows = get_binared(hashed_rows)
    zeros = count_zeros_in_bin_num_rule(binary_hashed_rows)
    mmax = max(zeros)
    print('!')


if __name__ == '__main__':
    main()
