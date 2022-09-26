import concurrent
import datetime
import logging
import os
import multiprocessing
import queue
import sqlite3
import sys
import textwrap
import threading

from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from copy import copy
from ctypes import c_char_p
from multiprocessing.managers import ValueProxy
from threading import Event
from time import sleep
from typing import Union

from alive_progress import alive_bar
from retry import retry

import emjr
import sql
from toxicity_measure import count_then_measure_post

logging.basicConfig(
     level=logging.WARNING,
     format= '[%(asctime)s] %(levelname)s - %(message)s',
     datefmt='%H:%M:%S'
 )
logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
SKIP_TOPICS = ('https://www.econjobrumors.com/topic/about-ejmr', 'https://www.econjobrumors.com/topic/request-a-thread-to-be-deleted-here')


def db_consumer(q: queue.Queue, stop_event: multiprocessing.Event, db_name:Union[str, ValueProxy], completed: ValueProxy, total: ValueProxy, current_text:ValueProxy):
    logger.debug(f"DB Consumer [{os.getpid()}] started")

    try:
        with sqlite3.connect(db_name.value, detect_types=sqlite3.PARSE_DECLTYPES) as con:
            while True:
                try:
                    post_dict_list = q.get(timeout=5)

                    if not post_dict_list:
                        completed.value += 1
                        continue

                    topic_title = emjr.collect_topic_title(
                        post_dict_list[0].get("url").strip()
                    )

                    if any(skip_url in post_dict_list[0].get("url").strip() for skip_url in SKIP_TOPICS):
                        completed.value += 1
                        continue

                    total.value += len(post_dict_list)

                    topic_author = post_dict_list[0].get("author").strip()
                    topic_author_id = sql.create_author(con, topic_author)

                    for post_dict in post_dict_list:
                        author_code = post_dict.get("author").strip()
                        post_content:str = post_dict.get("post").strip()
                        created_at = post_dict.get("created_at")
                        link = post_dict.get("url").strip()
                        toxicity_dict = count_then_measure_post(post_content)
                        author_id = sql.create_author(con, author_code)
                        # print(topic_author, topic_author_id, topic_title, link)
                        topic_id = sql.create_topic(
                            con, topic_title, topic_author_id
                        )
                        topic_url_id = sql.create_topic_url(
                            con, link, topic_author_id, topic_id
                        )
                        #  print("type", type(toxicity_dict["toxicity"].item()))
                        sql.create_post(
                            con, post_content, author_id, topic_id, topic_url_id, created_at,
                            toxicity=toxicity_dict["toxicity"],
                            severe_toxicity=toxicity_dict["severe_toxicity"],
                            obscene=toxicity_dict["obscene"],
                            identity_attack=toxicity_dict["identity_attack"],
                            insult=toxicity_dict["insult"],
                            threat=toxicity_dict["threat"],
                            sexual_explicit=toxicity_dict["sexual_explicit"]

                        )
                        post_text = textwrap.shorten(
                            post_content, width=40, placeholder="..."
                        ).ljust(40)
                        total_posts = sql.count_posts(con)
                        text = f"Total Posts: {str(total_posts): <7} Tasks Complete: {str(completed.value): <7} Total Tasks: {str(total.value): <7} Topic: {str(topic_id): <7} Author: {str(author_code): <4} {'[' + str(author_id) + ']': <7} Post: {post_text}"
                        logger.debug(f"DB Consumer [{os.getpid()}] {text}")
                        current_text.value = text

                        completed.value += 1

                    completed.value += 1

                except queue.Empty:
                    if stop_event.is_set():
                        logger.debug(f"DB Consumer [{os.getpid()}] is finished")
                        return
    except Exception:
        logger.exception(f'DB Consumer [{os.getpid()}] failed')
        db_consumer(q, stop_event, db_name, completed, total, current_text)

    logger.debug(f"DB Consumer [{os.getpid()}] Exiting")

def is_fresh(last_update:datetime.datetime, freshness:int):
    duration = datetime.datetime.now() - last_update
    duration_in_s = duration.total_seconds()
    return divmod(duration_in_s, 3600)[0] <= freshness

@retry(tries=3, delay=.1, backoff=1.5, jitter=(.1, 3), max_delay=30, logger=None)
def scrape_index(index_url, q: queue.Queue, completed: ValueProxy, total: ValueProxy, db_name:Union[str, ValueProxy], freshness:ValueProxy, scraped_pages:ValueProxy):
    short_url = textwrap.shorten(
        index_url, width=20, placeholder="..."
    )
    logger.debug(f"Index Scraper [{os.getpid()}] started. Index: {short_url}")
    try:
        with sqlite3.connect(db_name.value, detect_types=sqlite3.PARSE_DECLTYPES) as con:
            for url_dict in emjr.get_discussion_urls(index_url):
                if not sql.contains_url(con, url_dict["link"]) or is_fresh(url_dict['last_update'], freshness.value):
                    topic_pages = emjr.collect_topic_posts(
                        "https://www.econjobrumors.com/", url_dict["link"]
                    )

                    logger.debug(f'Index scraper [{os.getpid()}] add {len(topic_pages)} new topics')

                    q.put(topic_pages)
                    page_num = index_url.split('/')[-1]
                    if page_num.isdigit() and scraped_pages.value < int(page_num):
                        scraped_pages.value = int(page_num)

                    total.value += 1
                else:
                    logger.debug(f'Skipping {url_dict["link"]}')

    except Exception:
        logger.exception(f'Index scraper [{os.getpid()}] failed')
        raise
    else:
        logger.debug(f"Index Scraper [{os.getpid()}] completed")
        completed.value += 1

def _update_progress(all_complete:Event, completed: ValueProxy, total: ValueProxy, current_text:ValueProxy, scraped_pages:ValueProxy):

    while not all_complete.is_set():
        try:
            complete_percent = str(round((completed.value/total.value) * 100, 1))+"%"
            msg = f"Progress: {complete_percent: <7} Pages Scraped: {str(scraped_pages.value): <6} "+current_text.value

            sys.stdout.write('\r'+msg.ljust(250))
            sys.stdout.flush()

        except:
            logger.exception('Progress updater failed')
        finally:
            sleep(.5)

    logger.info("Progress updater complete")

if __name__ == "__main__":

    #######################
    # CHANGE THESE VALUES #
    #######################
    START = 1
    STOP = 15778
    DB_NAME = r'C:\Users\15083\Documents\EMJR\all_posts_continued_1-4m.db'
    FRESHNESS_AGE = 84 # The number in hours in the past a thread is considered fresh and should reevaluate
    #######################

    #if os.path.exists(DB_NAME):
    #    os.remove(DB_NAME)

    emjr.logger.setLevel(logger.level)
    m = multiprocessing.Manager()
    q = m.Queue()
    scrapping_complete_event = m.Event()
    db_name = m.Value(c_char_p, DB_NAME)
    current_text = m.Value(c_char_p, 'Waiting for scrapped data...')
    total = m.Value('i', STOP - START + 1)
    scraped_pages = m.Value('i', 0)
    completed = m.Value('i', 0)
    freshness = m.Value('i', FRESHNESS_AGE)

    all_complete = Event()
    scraper_futures = []
    db_consumers_futures = []
    scrapers = max(1,round(os.cpu_count() * 1))
    consumers = max(1,round(os.cpu_count() * .5))

    pool_exe = ProcessPoolExecutor
    if os.name == 'nt':
        pool_exe = ThreadPoolExecutor

    with ThreadPoolExecutor(scrapers) as scrapper_executor, pool_exe(consumers) as consumers_executor:
        prog_thread = threading.Thread(target=_update_progress, args=(all_complete, completed, total, current_text, scraped_pages), daemon=True)
        prog_thread.start()

        for i in range(consumers):
            db_consumers_futures.append(consumers_executor.submit(db_consumer, q, scrapping_complete_event, db_name, completed, total, current_text))

        try:
            db_consumers_futures[0].result(timeout=2)
        except concurrent.futures._base.TimeoutError:
            logger.info('DB Consumer passed startup check')
            pass

        for i in range(START, STOP + 1):
            if i == 1:
                url = "https://www.econjobrumors.com/"
            else:
                url = f"https://www.econjobrumors.com/page/{i}"

            scraper_futures.append(scrapper_executor.submit(scrape_index, url, q, completed, total, db_name, freshness, scraped_pages))

        try:
            scraper_futures[0].result(timeout=2)
        except concurrent.futures._base.TimeoutError:
            logger.info('Web scrapper passed startup check')
            pass

        logger.debug('Waiting for web scrappers to complete...')
        for fut in scraper_futures:
            try:
                fut.result()
                completed.value += 1
            except KeyboardInterrupt:
                pass
            except:
                logger.exception('Scraper failure')
        logger.info('Web scrappers finished')
        scrapping_complete_event.set()

        logger.debug('Waiting for DB consumers to complete...')
        for fut in db_consumers_futures:
            try:
                fut.result()
                completed.value += 1
            except KeyboardInterrupt:
                pass
            except:
                logger.exception('DB consumer failure')

        logger.info('DB consumers finished')
        try:
            all_complete.set()
        except KeyboardInterrupt:
            pass
    logger.debug('Application complete')