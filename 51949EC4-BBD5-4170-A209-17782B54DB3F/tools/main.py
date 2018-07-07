import urllib2
import Queue
from multiprocessing.dummy import Pool as ThreadPool

#gotta have that beautiful soup installed
from bs4 import BeautifulSoup

def crawl():

    searchQueue = Queue.Queue()
    peopleByEmail = {}

    __thread_count = 32
    __alphabet = ['a','b','c','d','e','f','g','h','i','j',
                  'k','l','m','n','o','p','q','r','s','t',
                  'u','v','w','x','y','z','.','0','1','2',
                  '3','4','5','6','7','8','9','_']

    def trackPeople(people):
        for i in people:
            email = i['email']
            if not peopleByEmail.has_key(email):
                peopleByEmail[email] = i

    def readAndParseHtml(emailPrefix):
        people = []

        url = 'https://people.nasa.gov/people/search?email=' + emailPrefix

        try:
            response = urllib2.urlopen(url)
        except:
            #if we have a timeout, or god forbid are throttled, add back to search queue to search again
            print("Failed crawling email, adding back to search queue: " + emailPrefix)
            searchQueue.put(emailPrefix)
            return []

        soup = BeautifulSoup(response.read(), 'html.parser')
        results = soup.body.div.contents[3].div.div.div
        warning = len(results.find_all('div')) == 3
        table = results.table

        if warning:
            for i in __alphabet:
                searchQueue.put(emailPrefix + i)
            return []
        else:
            count = 0
            for i in table.children:
                count += 1

                #skip header row
                if count <= 2:
                    continue
                else:
                    if i != "\n":
                        columns = i.find_all('td')
                        name = columns[0].text
                        email = columns[1].span.text
                        phone = columns[2].text
                        people.append({
                            'name': name.strip(),
                            'email': email.strip(),
                            'phone': phone.strip()
                        })

            return people

    def crawlForPrefix(emailPrefix):
        searchQueue.put(emailPrefix)

        while not searchQueue.empty():
            emails = []
            for i in range(__thread_count):
                if not searchQueue.empty():
                    emails.append(searchQueue.get())

            pool = ThreadPool(__thread_count)
            print("crawling email prefixes: " + '[%s]' % ', '.join(map(str, emails)))
            results = pool.map(readAndParseHtml, emails)
            pool.close()
            x = pool.join()

            for i in results:
                trackPeople(i)

    for i in __alphabet:
        crawlForPrefix(i)

    return peopleByEmail

peopleByEmail = crawl()

f = open('nasa_contacts.csv','w')
for key in peopleByEmail:
    person = peopleByEmail[key]
    try:
        f.write("\"" + person['email'] + "\",\"" + person['name'] + "\",\"" + person['phone'] + "\",\n")
    except:
        print("Failed to write this person")
f.close()