import json
import requests
from bs4 import BeautifulSoup
import os
import shutil
import threading
import locale

E_AMUSEMENT_BASE_URL = "https://p.eagate.573.jp"
ARCADE_SONGLIST_URL = E_AMUSEMENT_BASE_URL + "/game/sdvx/vi/music/index.html"
PC_SONGLIST_URL = E_AMUSEMENT_BASE_URL + "/game/eacsdvx/vi/music/index.html"
BASIC_COURSE_PACK_NAME = "最初からプレーできます"
BASIC_COURSE_PACK_ID = "1000000"
EAC_JACKET_PATH = "https://p.eagate.573.jp/game/sdvx/vi/common/jacket.html?img="
REQUEST_TIMEOUT = 7.5

def scrape():
    """Scrapes song metadata and jackets from the KONAMI website.
    
    Metadata is exported to `results.json`.
    Jackets are exported to the `jackets` folder.
    Returns the song metadata object.
    """

    print("Scraping song pack information...")
    pack_names_to_ids = _scrape_pack_names_to_ids()

    print("Scraping arcade songlist...")
    arcade_songlist = _scrape_single_songlist(ARCADE_SONGLIST_URL, pack_names_to_ids)

    print("Scraping PC songlist...")
    pc_songlist = _scrape_single_songlist(PC_SONGLIST_URL, pack_names_to_ids)

    print("Merging arcade and PC songlists...")
    songlist = _merge_songlists(arcade_songlist, pc_songlist)

    print("Scraping jacket art...")
    _scrape_jacket_art(songlist)

    print("Writing songlist to file...")

    results = {
        "songs": songlist,
        "packs": { v: k for k, v in pack_names_to_ids.items() }
    }
    with open("results.json", "w+", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)
    
    return results



def _scrape_pack_names_to_ids():
    """Scrapes the pack names and IDs from the PC song webpage."""
    page_html = _request_html(PC_SONGLIST_URL)
    pack_list = (page_html
        .find("select", {"name": "search_condition"})
        .find_all("option"))

    pack_names_to_ids = { BASIC_COURSE_PACK_NAME: BASIC_COURSE_PACK_ID }
    pack_names_to_ids.update({ option.text: option["value"]
        for option in pack_list if option.text != "" })

    return pack_names_to_ids


def _scrape_single_song(song_html, songlist, pack_names_to_ids):
    """Scrapes a single song from the song HTML."""

    song_p_tags = song_html.find("div", class_="info").find_all("p", recursive=False)
    title = song_p_tags[0].text.removesuffix("(EXIT TUNES)")
    artist = song_p_tags[1].text

    print(f"Scraping metadata for {title} by {artist}...")
    categories = [s.text for s in song_html.find_all("div", class_="genre")]
    pack_name = song_p_tags[2].text if len(song_p_tags) > 2 else None
    pack_id = pack_names_to_ids.get(pack_name)

    detailed_page_url = E_AMUSEMENT_BASE_URL + song_html.find("a")["href"]
    detailed_html = _request_html(detailed_page_url)
    difficulties = (detailed_html
        .find("div", class_="inner")
        .find_all("div", class_="cat"))

    charts = []
    for diff_html in difficulties:
        ps = diff_html.find_all("p")
        diff_name = ps[0]["class"][0].upper()
        charts.append({
            "diff": diff_name,
            "level": int(ps[0].text),
            "jacket": diff_html.find("img")["src"].rsplit("=", 1)[1],
            "effector": ps[2].text,
            "illustrator": ps[1].text,
        })

    songlist.append({
        "id": None,
        "title": title,
        "artist": artist,
        "charts": charts,
        "pack": pack_id,
        "categories": categories,
    })
    

def _scrape_single_page(page_html, songlist, pack_names_to_ids):
    """Takes a single HTML page for songs, scrapes the song metadata and appends the songs to `songlist`."""

    songs_html = (page_html.body
        .find("div", id="music-result")
        .find_all("div", class_="music", recursive=False)
    )

    threads = []
    for song_html in songs_html:
        thread = threading.Thread(target=_scrape_single_song,
            args=(song_html, songlist, pack_names_to_ids))
        threads.append(thread)

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def _scrape_single_songlist(url, pack_names_to_ids):
    """Scrapes the songlist found at `url`."""

    songlist = []
    def thread_func(url, songlist, pack_names_to_ids):
        page_html = _request_html(url)
        _scrape_single_page(page_html, songlist, pack_names_to_ids)

    page_html = _request_html(url)
    num_pages = len(page_html.find("select", id="search_page").find_all("option"))

    _scrape_single_page(page_html, songlist, pack_names_to_ids)

    threads = []
    for i in range(2, num_pages + 1):
        page_url = f"{url}?page={i}"
        thread = threading.Thread(target=thread_func,
            args=(page_url, songlist, pack_names_to_ids))
        threads.append(thread)

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    return songlist


def _merge_songlists(arcade_songlist, pc_songlist):
    """Merges and sorts the arcade and PC songlists."""

    # Prioritizes arcade metadata when there are conflicts.
    merged_songlist = { song["title"]: song for song in arcade_songlist }
    for song in pc_songlist:
        title = song["title"]
        if title in merged_songlist:
            merged_songlist[title]["pack"] = song["pack"]
        else:
            merged_songlist[title] = song

    merged_songlist = list(merged_songlist.values())
    # Sorts the songs by highest level decreasing, then alphabetically.
    merged_songlist.sort(key=lambda song: (
        -max([chart["level"] for chart in song["charts"]]),
        locale.strxfrm(song["title"]),
    ))
    # Add song IDs.
    for i, song in enumerate(merged_songlist):
        song["id"] = i

    return merged_songlist


def _scrape_single_jacket(jacket_num, konami_jacket_id):
    """Downloads a single jacket and places it in the folder `jackets`."""

    jacket_path = f"./jackets/{jacket_num}.jpg"
    image_url = EAC_JACKET_PATH + konami_jacket_id
    while True:
        try:
            r = requests.get(image_url, stream=True, timeout=REQUEST_TIMEOUT)
            r.raw.decode_content = True
            with open(jacket_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)
            return
        except:
            pass 


def _scrape_jacket_art(songlist):
    """Downloads the jacket art to each song."""

    if os.path.isdir("./jackets"):
        shutil.rmtree("./jackets")
    os.mkdir("./jackets")

    jacket_nums_to_ids = {}
    i = 0
    for song in songlist:
        for chart in song["charts"]:
            jacket_nums_to_ids[i] = chart["jacket"]
            chart["jacket"] = i
            i += 1

    threads = []
    for song in songlist:
        for chart in song["charts"]:
            jacket_num = chart["jacket"]
            konami_jacket_id = jacket_nums_to_ids[jacket_num]
            thread = threading.Thread(
                target=_scrape_single_jacket,
                args=(jacket_num, konami_jacket_id),
            )
            threads.append(thread)
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def _request_html(url):
    """Takes a song page URL and returns the HTML of the page."""

    while True:
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            result = BeautifulSoup(response.content.decode("shiftjis"), "html.parser")
            assert len(list(result.find_all("div"))) > 0
            return result
        except:
            pass


if __name__ == "__main__":
    locale.setlocale(locale.LC_ALL, "en")
    scrape()