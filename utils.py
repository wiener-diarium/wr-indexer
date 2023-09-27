def make_title(row):
    date = row["Ausgabe"].split()[0]
    y, m, d = date.split("-")
    title = f"Wienerisches Diarium, {date}, Nummer {row['Nummer']}, Seite {row['Seite']}, laufende Nr. {row['ID']}"
    return title


def find_broader(row, lookup_table, index_nr):
    item = row["full_code"][:index_nr]
    try:
        first_match = lookup_table[item]
    except KeyError:
        first_match = ""
    return first_match


def make_wr_id(row):
    date = row["Ausgabe"].split()[0]
    page = f"{row['Seite']:0>2}"
    return f"wr_{date}__{page}"


def set_default(obj):
    if isinstance(obj, set):
        return list(obj)
    raise TypeError
