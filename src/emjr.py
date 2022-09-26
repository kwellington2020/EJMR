import datetime
import logging

from urllib.request import urlopen

import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from retry import retry
import re
from natsort import natsorted
from fake_useragent import UserAgent
from torpy.http.requests import TorRequests

ua = UserAgent()
session = requests.Session()
session.max_redirects = 60
adapter = HTTPAdapter(pool_maxsize=150, max_retries=3)
session.mount('https://', adapter)

logging.basicConfig(
     level=logging.INFO,
     format= '[%(asctime)s] %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
 )
logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)
logging.getLogger("urllib3").propagate = False
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib").setLevel(logging.ERROR)
logging.getLogger("torpy").setLevel(logging.CRITICAL)
logging.getLogger("fake_useragent").setLevel(logging.CRITICAL)


def _get_headers():
    return {
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'en-US,en;q=0.8',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': str(ua.random),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
    }

@retry(tries=3, delay=.5, backoff=1.2, jitter=(.1, 3), max_delay=10, logger=None)
def _get(url):
    global session
    try:
        @retry(tries=7, delay=0.1, backoff=1.2, max_delay=4, logger=None)
        def _reg_url(url):
            return urlopen(url, timeout=40)

        try:
            response = _reg_url(url)
            return type('obj', (object,), {'text': response.read().decode()})
        except Exception:
            pass

        try:
            return session.get(url, allow_redirects=True, headers=_get_headers(), timeout=(60, 60))
        except Exception:
            session.cookies.clear()

        try:
            return session.get(url, allow_redirects=True, headers=_get_headers(), timeout=(60, 60))
        except Exception:
            logger.debug('Falling back to tor')
            with TorRequests() as tor_requests:
                with tor_requests.get_session(retries=4) as sess:
                   return sess.get(url)
    except Exception:
        #logger.debug('Get request failed', exc_info=True)
        raise

@retry(tries=10, delay=5, backoff=1.5, jitter=(.1, 3), max_delay=30, logger=None)
def collect_posts(url):
    """collect posts to record EMJR comments/posts and attribute to author (threadauthor)

    Args:
        url (str): link to specific discussion on EMJR

    Returns:
        list of dictionaries and each dictionary has post and threadauthor {"author": str, "post": str}
    """
    #print("url ->", url)
    fhand = _get(url)
    html_content = fhand.text

    soup = BeautifulSoup(html_content, 'html.parser')
    a = {"class": "post"}
    elements = soup("div", attrs=a)
    authorattributes = {"class": "threadauthor"}
    to_return = []
    #make for loop
    for element in elements:
        threadpost = element.parent
        poststuff = threadpost.find("div", {"class": "poststuff"})
        created_at = get_dates(poststuff.text)
        parent = element.parent.parent
        author = parent.find("div", authorattributes).find("small").text
        post = element.text
        post_dictionary = {"author": author, "post": post, "created_at": created_at}
        to_return.append(post_dictionary)
    return to_return
#print(collect_posts(html_content))

def get_dates(string_date: str):
  #converts the string from the href attribute to a python date time object
    string_date = string_date.split("ago #")[0].strip()

    time_value = int(re.findall(r"\d+", string_date)[0].strip())
    unit_time = ''.join(c for c in string_date if not c.isdigit()).strip()
    #print(time_value, unit_time)
    if unit_time in "seconds":
        creation_date = datetime.datetime.now() - datetime.timedelta(seconds=time_value)
    elif unit_time in "minutes":
        creation_date = datetime.datetime.now() - datetime.timedelta(minutes=time_value)
    elif unit_time in "hours":
        creation_date = datetime.datetime.now() - datetime.timedelta(hours=time_value)
    elif unit_time in "days":
        creation_date = datetime.datetime.now() - datetime.timedelta(days=time_value)
    elif unit_time in "weeks":
        creation_date = datetime.datetime.now() - datetime.timedelta(weeks=time_value)
    elif unit_time in "months":
        creation_date = datetime.datetime.now() - datetime.timedelta(days=time_value*30)
    elif unit_time in "years":
        creation_date = datetime.datetime.now() - datetime.timedelta(days=time_value*365)
    #print(string_date, unit_time, f">{unit_time}<")
    return creation_date



#print(elements)
"""
# step 1 - examine the website html structure

# step 2 - find pattern in structure of which url is on main page lead to thread discussions
#  looking for   table id = 'latest'
                  tbody
                  tr
                    a

# step 3 - use pattern as filter to extract urls via beautiful soup

# step 4 - once accumulated urls, urls will be put into list

# step 5 - return the list of urls
"""
"""
<a class="page-numbers" href="https://www.econjobrumors.com/topic/official-thread-for-brazilian-economists/page/3" title="Page 3">3</a>
"""


url = 'https://www.econjobrumors.com/page/2'
"""
<a class="page-numbers" href="https://www.econjobrumors.com/topic/i-dont-understand-the-kolev-threads/page/2" title="Page 2">2</a>
"""


@retry(tries=10, delay=5, backoff=1.5, jitter=(.1, 3), max_delay=30, logger=None)
def get_discussion_urls(url):
    """ gets all urls of all individual threads for each page on url referenced

    example url https://www.econjobrumors.com/

    or https://www.econjobrumors.com/page/2

    Args:
        url (str): main page on EMJR where all threads are listed

    Returns:
        list of urls and each url is location of thread in EMJR
    """
    #print("url ->", url)
    response = _get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup("table", {"id": "latest"})
  #  print(table)
    link_list = []
    if table:
        rows = table[0].find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            topic_info = {"pages": 1}

            for cell in cells:

                if cell.get("class") == ['num', 'l']:
                    date = get_dates(cell.find('a').text)
                    topic_info["last_update"] = date

                if not cell.get("class"):
                    link_elements = cell.find_all("a")

                    for link_element in link_elements:

                        if link_element.get("class") == ["page-numbers"]:
                            page_number = int(link_element.text.replace(',', ''))
                            topic_info["pages"] = max(topic_info["pages"], page_number)
                        if not link_element.get("class") and not link_element.get(
                                "title"):
                            link = link_element.get("href")
                            topic_info["link"] = link
                            link_list.append(topic_info)
    return link_list

@retry(tries=10, delay=5, backoff=1.5, jitter=(.1, 3), max_delay=30, logger=None)
def topic_urls(base_url, url):
    """<span class="page-numbers current" title="Page 3">3</span>"""
    topic_pages = set()
    topic_pages.add(url)
    #print("url ->", url)
    response = _get(url)
    #time.sleep(1)
    html_content = response.text
    soup = BeautifulSoup(html_content, 'html.parser')

    a = {"class": "page-numbers"}
    elements = soup("a", attrs=a)

    for page_element in elements:
        if page_element.text.strip().isdigit():
            end = int(page_element.text.strip())
            for i in range(2, end + 1):
                link = re.sub(r"\/\d+$", f"/{i}", base_url + page_element.get("href"))

                topic_pages.add(link)
    topic_pages = natsorted(list(topic_pages))
    topic_pages.insert(0, topic_pages.pop())

    # print(page_element.get("href"))
    #print(topic_pages)
    return topic_pages
#print(topic_urls('https://www.econjobrumors.com/', 'https://www.econjobrumors.com/topic/princeton-jmcs-2019-2020'))


@retry(tries=10, delay=5, backoff=1.5, jitter=(.1, 3), max_delay=30, logger=None)
def get_urls(start, stop):
    #all_discussion_urls = []
    for i in range(start, stop + 1):
        if i == 1:
            url = "https://www.econjobrumors.com/"
        else:
            url = f"https://www.econjobrumors.com/page/{i}"
        for url_dict in get_discussion_urls(url):
            yield url_dict
        #all_discussion_urls.extend(discussion_urls)
    #return all_discussion_urls
#print(get_discussion_urls("https://www.econjobrumors.com/"))


"""result = get_urls(1, 2)
for url in result:
    post_urls = collect_posts(url)

    print(post_urls)

#result = combine_discussion_urls(url)
#print(result)"""


"""import urllib.request  #get_urls

fhand = urllib.request.urlopen('https://www.econjobrumors.com/topic/getting-published-well-out-of-my-league')  

html_content = fhand.read().decode('utf-8')

print(html_content)"""


#for line in fhand:
#    print(line.decode().strip())

# Code: http://www.py4e.com/code3/urllib1.py


"""#import urllib.request, urllib.parse, urllib.error

fhand = urllib.request.urlopen('http://data.pr4e.org/romeo.txt')  #collect_posts   combine_discussion_url

counts = dict()
for line in fhand:
    words = line.decode().split()
    for word in words:
        counts[word] = counts.get(word, 0) + 1
print(counts)
"""

# Code: http://www.py4e.com/code3/urlwords.py


@retry(tries=10, delay=5, backoff=1.5, jitter=(.1, 3), max_delay=30, logger=None)
def collect_topic_posts(base_url, url):
    all_posts = []
    try:
        for url in topic_urls(base_url, url):
            posts = collect_posts(url)
            for post in posts:
                post["url"] = url
            all_posts.extend(posts)
    except requests.exceptions.TooManyRedirects:
        pass
       #print(f"error {url} too many redirects")
    return(all_posts)
#topic_pages = collect_topic_posts("https://www.econjobrumors.com/", "https://www.econjobrumors.com/topic/has-anyone-published-their-way-to-a-professorship-without-a-phd")
#print(topic_pages)


@retry(tries=10, delay=5, backoff=1.5, jitter=(.1, 3), max_delay=30, logger=None)
def collect_topic_title(url):
    # <h2 class="topictitle">Deve Gowda</h2>
    response = _get(url)
    html_content = response.text

    soup = BeautifulSoup(html_content, 'html.parser')
    a = {"class": "topictitle"}
    elements = soup("h2", attrs=a)
    title = elements[0].text
    return title
