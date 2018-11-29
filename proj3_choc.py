import sqlite3
import csv
import json

# proj3_choc.py
# You can change anything in this file you want as long as you pass the tests
# and meet the project requirements! You will need to implement several new
# functions.

# Part 1: Read data from CSV and JSON into a new database called choc.db
DBNAME = 'choc.db'
BARSCSV = 'flavors_of_cacao_cleaned.csv'
COUNTRIESJSON = 'countries.json'

conn = sqlite3.connect(DBNAME)
cur = conn.cursor()

# Creat tables
statement = '''DROP TABLE IF EXISTS 'Countries'; '''
cur.execute(statement)
conn.commit()
statement = '''
    CREATE TABLE 'Countries' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Alpha2' TEXT NOT NULL,
                'Alpha3' TEXT NOT NULL,
                'EnglishName' TEXT,
                'Region' TEXT,
                'Subregion' TEXT,
                'Population' INTEGER,
                'Area' REAL
            );
'''
cur.execute(statement)
conn.commit()

statement = '''DROP TABLE IF EXISTS 'Bars'; '''
cur.execute(statement)
conn.commit()
statement = '''
    CREATE TABLE 'Bars' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Company' TEXT NOT NULL,
                'SpecificBeanBarName' TEXT NOT NULL,
                'REF' TEXT NOT NULL,
                'ReviewDate' TEXT NOT NULL,
                'CocoaPercent' REAL NOT NULL,
                'CompanyLocation'TEXT,
                'CompanyLocationId' INTEGER REFERENCES Countries(Id),
                'Rating' REAL NOT NULL,
                'BeanType' TEXT,
                'BroadBeanOrigin'TEXT,
                'BroadBeanOriginId' INTEGER REFERENCES Countries(Id)
            );
'''
cur.execute(statement)
conn.commit()


# Populates choc.db database using csv files
bars_file = open(COUNTRIESJSON, 'r', encoding="utf8")
bars_contents = bars_file.read()
countries = json.loads(bars_contents)
bars_file.close()
for data in countries:
    insertion = (None,data["alpha2Code"],data["alpha3Code"],data['name'],data['region'],data['subregion'],data['population'],data['area'])
    statement = 'INSERT INTO "Countries" '
    statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    cur.execute(statement, insertion)
    conn.commit()

# update_country_id
with open(BARSCSV,'r', encoding="utf8") as bars:
    csvReader = csv.reader(bars)
    bar = list(csvReader)
for row in bar[1:]:
    row[4]=row[4].replace('%','')
    insertion = (None, row[0], row[1], row[2], row[3], row[4],row[5],None,row[6],row[7],row[8],None)
    statement = 'INSERT INTO "Bars" '
    statement += 'VALUES (?,?,?,?, ?, ?, ?, ?,?,?,?,?)'
    cur.execute(statement, insertion)
    conn.commit()

statement = '''
    SELECT EnglishName, Id
    FROM Countries
'''
countries = cur.execute(statement)
countries_dict = {}
for row in countries:
    countries_dict[str(row[0])] = str(row[1])
for row in countries_dict:
    statement = '''
        UPDATE Bars
        SET CompanyLocationId = ?
        WHERE CompanyLocation = ?
    '''
    cur.execute(statement, (countries_dict[row], row,))
    statement = '''
        UPDATE Bars
        SET BroadBeanOriginId = ?
        WHERE BroadBeanOrigin = ?
    '''
    cur.execute(statement, (countries_dict[row], row,))
conn.commit()



# Part 2: Implement logic to process user commands
def process_command(command):
    command_content = command.split()

    if 'bars' in command_content:
        statement = processing_bars(command_content)
    if 'countries' in command_content:
        statement = processing_countries(command_content)
    if 'regions' in command_content:
        statement = processing_regions(command_content)
    if 'companies' in command_content:
        statement = processing_companies(command_content)

    res = []
    # print(statement)
    search = cur.execute(statement)
    for row in search:
        res.append(row)
    return res

def processing_bars(command_content):
    statement = '''
        SELECT SpecificBeanBarName, Company,CompanyLocation, round(Rating,2),CocoaPercent,BroadBeanOrigin
        FROM Bars
        Join Countries
        on Countries.EnglishName = bars.CompanyLocation
    '''
    parameters_sellers = ''
    parameters_order = ''' ORDER BY Bars.Rating DESC'''
    parameters_limit = ''' limit 10'''

    for each in command_content:
        if 'sellcountry' in each:
            x,y = each.split('=')
            Statement_EnglishName = '''
                SELECT EnglishName
                FROM Countries
                WHERE Alpha2 = ?
            '''
            result = cur.execute(Statement_EnglishName, (str(y),))
            for i in result:
                name = i[0]
            parameters_sellers = ' WHERE CompanyLocation = "'+str(name)+'"'
        elif 'sellregion'in each:
            x,y = each.split('=')
            statement_EnglishName = '''
                SELECT EnglishName
                FROM Countries
                WHERE Region = ?
            '''
            result = cur.execute(statement_EnglishName, (str(y),))
            list=[]
            for name in result:
                list.append(name[0])
            parameters_sellers =' WHERE CompanyLocation in '+'{}'.format(tuple(list))
        elif 'sourcecountry'in each:
            x,y = each.split('=')
            statement_EnglishName = '''
                SELECT EnglishName
                FROM Countries
                WHERE Alpha2 = ?
            '''
            result = cur.execute(statement_EnglishName, (str(y),))
            for i in result:
                name = i[0]
            parameters_sellers =' WHERE BroadBeanOrigin = "'+str(name)+'"'
        elif 'sourceregion' in each:
            x,y = each.split('=')
            statement_EnglishName = '''
                SELECT EnglishName
                FROM Countries
                WHERE Region = ?
            '''
            result = cur.execute(statement_EnglishName, (str(y),))
            list=[]
            for name in result:
                list.append(name[0])
            parameters_sellers =' WHERE BroadBeanOrigin in '+'{}'.format(tuple(list))
        elif 'cocoa' in each:
            parameters_order =''' ORDER BY CocoaPercent DESC'''
        elif 'top' in each:
            x,y = each.split('=')
            parameters_limit =' limit '+str(y)
        elif 'bottom' in each:
            x,y = each.split('=')
            parameters_limit = ' limit '+str(y)
            parameters_order = parameters_order.replace('DESC',' ')

    statement += parameters_sellers
    statement += parameters_order
    statement += parameters_limit
    return statement

def processing_countries(command_content):

    parameters_num_limit = ' having count(Bars.Id) > 4'
    parameters_group =' GROUP BY Countries.EnglishName'
    parameters_sources = ' Bars.CompanyLocation = Countries.EnglishName'
    parameters_region = ''
    parameters_rating = ' round(avg(Bars.Rating),1)'
    parameters_order = ' ORDER By avg(Bars.Rating) DESC'
    parameters_limit = ' limit 10'

    for each in command_content:
        if 'top' in each:
            x,y = each.split('=')
            parameters_limit = ' limit '+str(y)
        elif 'bottom' in each:
            x,y = each.split('=')
            parameters_limit = ' limit '+str(y)
            parameters_order = parameters_order.replace('DESC',' ')
        elif 'region' in each:
            x,y = each.split('=')
            parameters_region = ' WHERE Countries.Region = "'+str(y)+'"'
        elif 'sources' in each:
            parameters_sources = ' Bars.BroadBeanOrigin = Countries.EnglishName'
        elif 'cocoa' in each:
            parameters_rating = ' round(avg(Bars.CocoaPercent),0)'
            parameters_order = ' ORDER BY avg(Bars.CocoaPercent) DESC'
        elif 'bars_sold' in each:
            parameters_rating = ' count(Bars.Id)'
            parameters_order = ' ORDER BY count(Bars.Id) DESC'

    statement = "SELECT Countries.EnglishName, Countries.Region, " + parameters_rating + " FROM Countries Join Bars ON" + parameters_sources
    statement += parameters_region + parameters_group + parameters_num_limit + parameters_order + parameters_limit
    return statement

def processing_regions(command_content):
    for each in command_content:
        parameters_source = ' Bars.CompanyLocation = Countries.EnglishName'
        parameters_rating = ' round(avg(bars.Rating),1)'
        parameters_order = ' ORDER By avg(bars.Rating) DESC'
        parameters_limit = ' limit 10'
        parameters_num = ' having count(Bars.Id) >= 4'
        parameters_group =' GROUP BY Countries.Region'
        for each in command_content:
            if 'top' in each:
                x,y = each.split('=')
                parameters_limit = ' limit '+str(y)
            elif 'bottom' in each:
                x,y = each.split('=')
                parameters_limit = ' limit '+str(b)
                parameters_order = parameters_order.replace('DESC',' ')
            elif 'sources' in each:
                parameters_source = ' Bars.BroadBeanOrigin = Countries.EnglishName'
            elif 'cocoa' in each:
                parameters_rating = ' round(avg(Bars.CocoaPercent),0)'
                parameters_order = ' ORDER BY avg(Bars.CocoaPercent) DESC'
            elif 'bars_sold' in each:
                parameters_rating = ' count(Bars.Id)'
                parameters_order = ' ORDER BY count(Bars.Id) DESC'

    statement = "SELECT Countries.Region," + parameters_rating +" FROM Countries Join Bars on" + parameters_source
    statement += parameters_group + parameters_num + parameters_order + parameters_limit
    return statement


def processing_companies(command_content):

    Statement = '''
        SELECT Bars.Company, Bars.CompanyLocation,round(avg(bars.Rating),1)
        FROM Bars
        Join Countries
        on bars.CompanyLocation = Countries.EnglishName
    '''
    parameters_order = ' ORDER BY avg(bars.Rating) DESC'
    parameters_limit = ' limit 10'
    parameters_region = ''
    parameters_country = ''
    parameters_num = ' having count(Bars.Id) > 4'
    parameters_group =' GROUP BY Bars.Company'

    for each in command_content:
        if 'top' in each:
            x,y = each.split('=')
            parameters_limit =' limit '+str(y)

        elif 'bottom' in each:
            x,y = each.split('=')
            parameters_limit = ' limit '+str(y)
            parameters_order = parameters_order.replace('DESC',' ')

        elif  'country' in each:
            x,y = each.split('=')
            Statement_EnglishName = '''
                SELECT EnglishName
                FROM Countries
                WHERE Alpha2 = ?
            '''
            result = cur.execute(Statement_EnglishName, (str(y),))
            for i in result:
                name = i[0]
            parameters_country = ' WHERE CompanyLocation = "'+str(name)+'"'

        elif 'region' in each:
            x,y = each.split('=')
            parameters_region= ' WHERE Countries.Region ="'+str(y)+'"'

        elif 'cocoa' in each:
            Statement = '''
                SELECT Company, CompanyLocation,round(avg(CocoaPercent),2)
                FROM Bars
                Join Countries
                on bars.CompanyLocation = Countries.EnglishName
            '''
            parameters_order = ' ORDER BY avg(CocoaPercent) DESC'

        elif 'bars_sold' in each:
            Statement = '''
                SELECT Company, CompanyLocation,count(Bars.Id)
                FROM Bars
                Join Countries
                on bars.CompanyLocation = Countries.EnglishName
            '''
            parameters_order = ' ORDER BY count(Bars.Id) DESC'

    statement = Statement + parameters_region + parameters_country + parameters_group + parameters_num+ parameters_order+ parameters_limit
    return statement

def load_help_text():
    with open('help.txt') as f:
        return f.read()

# Part 3: Implement interactive prompt. We've started for you!
total_list =['countries','bars','regions','companies','top','bottom','ratings','cocoa','bars_sold','sources','sellers','region','country','sourceregion','sourcecountry','sellregion','sellcountry']
def interactive_prompt():
    help_text = load_help_text()
    response = ''
    while response != 'exit':
        response = input('Enter a command: ')

        if response == 'help':
            print(help_text)
        command = response.split()
        if 'countries' in command:
            handlling_countries(response)
        elif 'bars' in command:
            handlling_bars(response)
        elif 'companies' in command:
            handlling_companies(response)
        elif 'regions' in command:
            handlling_regions(response)
        elif response == 'exit':
            print('bye~')
        else:
            print('Command not recognized: {}'.format(response))

def handlling_countries(response):
    command = response.split()
    for each in command:
        x = each.split('=')
        if x[0] not in total_list:
            print('Command not recognized: {}'.format(response))
            return

    for each in command:
        res = process_command(response)
        for i in res:
                s = [" %-12s ", '   '," %-12s ", '   '," %-12s ", '   ']
                cnt = 0
                for j in i:
                    cnt += 1
                    if len(str(j))>12:
                        s[2*cnt-1]='...'
                if 'cocoa' in res:
                    print(''.join(s) % (str(i[0])[:12],str(i[1])[:12],str(i[2])+'%'))
                else:
                    print(''.join(s) % (str(i[0])[:12],str(i[1])[:12],str(i[2])))

def handlling_bars(response):
    command = response.split()
    for each in command:
        x = each.split('=')
        if x[0] not in total_list:
            print('Command not recognized: {}'.format(response))
            return

    for each in command:
        x = each.split('=')
        res = process_command(response)
        for i in res:
                s = [" %-12s ", '   '," %-12s ", '   '," %-12s ", '   '," %-3s ", ' '," %-3s ", ' '," %-12s ", '   ']
                cnt = 0
                for j in i:
                    cnt += 1
                    if len(str(j))>12:
                        s[2*cnt-1]='...'
                print(''.join(s) % (str(i[0])[:12],str(i[1])[:12],str(i[2])[:12],str(i[3])[:3],str(i[4])[:3]+'%',str(i[5])[:12]))

def handlling_companies(response):
    command = response.split()
    for each in command:
        x = each.split('=')
        if x[0] not in total_list:
            print('Command not recognized: {}'.format(response))
            return

    for each in command:
        x = each.split('=')
        res = process_command(response)
        for i in res:
                s = [" %-12s ", '   '," %-12s ", '   '," %-12s ", '   ']
                cnt = 0
                for j in i:
                    cnt += 1
                    if len(str(j))>12:
                        s[2*cnt-1]='...'
                if 'cocoa' in res:
                    print(''.join(s) % (str(i[0])[:12],str(i[1])[:12],str(i[2])+'%'))
                else:
                    print(''.join(s) % (str(i[0])[:12],str(i[1])[:12],str(i[2])))

def handlling_regions(response):
    command = response.split()
    for each in command:
        x = each.split('=')
        if x[0] not in total_list:
            print('Command not recognized: {}'.format(response))
            return
    for each in command:
        x = each.split('=')
        res = process_command(response)
        for i in res:
                s = [" %-12s ", '   '," %-12s ", '   ']
                cnt = 0
                for j in i:
                    cnt += 1
                    if len(str(j))>12:
                        s[2*cnt-1]='...'
                if 'cocoa' in res:
                    print(''.join(s) % (str(i[0])[:12],str(i[1])[:12]+'%'))
                else:
                    print(''.join(s) % (str(i[0])[:12],str(i[1])[:12]))

# Make sure nothing runs or prints out when this file is run as a module
if __name__=="__main__":
    interactive_prompt()
