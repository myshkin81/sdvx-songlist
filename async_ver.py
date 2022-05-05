import json
from bs4 import BeautifulSoup
import os
import shutil
import asyncio
import aiohttp
import aiofiles

E_AMUSEMENT_BASE_URL = "https://p.eagate.573.jp"
ARCADE_SONGLIST_URL = E_AMUSEMENT_BASE_URL + "/game/sdvx/vi/music/index.html"
PC_SONGLIST_URL = E_AMUSEMENT_BASE_URL + "/game/eacsdvx/vi/music/index.html"
BASIC_COURSE_PACK_NAME = "最初からプレーできます"
BASIC_COURSE_PACK_ID = "1000000"
EAC_JACKET_PATH = "https://p.eagate.573.jp/game/sdvx/vi/common/jacket.html?img="
TIMEOUT = aiohttp.ClientTimeout(total=7.5)


async def main():
    # Doesn't reuse TCP connections. This has a performance hit, but means
    # KONAMI servers don't disconnect me.
    connector = aiohttp.TCPConnector(force_close=True, limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        print("Scraping song pack information...")
        pack_names_to_ids = await scrape_pack_names_to_ids(session)

        print("Scraping arcade songlist...")
        arcade_songlist = await scrape_single_songlist(
            session,
            ARCADE_SONGLIST_URL,
            pack_names_to_ids,
        )

        print("Scraping PC songlist...")
        pc_songlist = await scrape_single_songlist(
            session,
            PC_SONGLIST_URL,
            pack_names_to_ids,
        )

        print("Merging arcade and PC songlists...")
        songlist = merge_songlists(arcade_songlist, pc_songlist)

        jacket_nums_to_konami_ids = {}
        i = 0
        for song in songlist:
            for chart in song["charts"]:
                jacket_nums_to_konami_ids[i] = chart["jacket"]
                chart["jacket"] = i
                i += 1

        results = {
            "songs": songlist,
            "packs": { v: k for k, v in pack_names_to_ids.items() }
        }

        print("Writing songlist to file...")
        with open("results.json", "w+", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=4)

        print("Scraping jacket art...")
        await scrape_jacket_art(session, songlist, jacket_nums_to_konami_ids)

        return results


async def scrape_pack_names_to_ids(session):
    """Scrapes the pack names and IDs from the PC song webpage."""
    page_html = await request_html(session, PC_SONGLIST_URL)
    pack_list = (page_html
        .find("select", {"name": "search_condition"})
        .find_all("option"))

    pack_names_to_ids = { BASIC_COURSE_PACK_NAME: BASIC_COURSE_PACK_ID }
    pack_names_to_ids.update({ option.text: option["value"]
        for option in pack_list if option.text != "" })

    return pack_names_to_ids


async def scrape_single_song(session, basic_song_html, pack_names_to_ids):
    """Scrapes a single song from the song HTML, performing an additional network request to get detailed song information."""

    song_p_tags = basic_song_html.find("div", class_="info").find_all("p", recursive=False)
    title = song_p_tags[0].text.removesuffix("(EXIT TUNES)")
    artist = song_p_tags[1].text

    categories = [s.text for s in basic_song_html.find_all("div", class_="genre")]
    pack_name = song_p_tags[2].text if len(song_p_tags) > 2 else None
    pack_id = pack_names_to_ids.get(pack_name)

    detailed_page_url = E_AMUSEMENT_BASE_URL + basic_song_html.find("a")["href"]
    detailed_html = await request_html(session, detailed_page_url)
    print(f"Scraped metadata for {title} by {artist}...")
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

    return {
        "id": None,
        "title": title,
        "artist": artist,
        "charts": charts,
        "pack": pack_id,
        "categories": categories,
    }


async def scrape_single_page(session, page_html, pack_names_to_ids):
    """Takes a single HTML page for songs and scrapes the song metadata."""

    songs_html = (page_html.body
        .find("div", id="music-result")
        .find_all("div", class_="music", recursive=False)
    )

    tasks = [scrape_single_song(session, s, pack_names_to_ids)
        for s in songs_html]
    return await asyncio.gather(*tasks)


async def scrape_single_songlist(session, url, pack_names_to_ids=None):
    """Scrapes the songlist found at `url`."""

    async def thread_func(page_num):
        page_html = await request_html(session, url, params={"page": page_num})
        return await scrape_single_page(session, page_html, pack_names_to_ids)

    page_1_html = await request_html(session, url)
    num_pages = len(page_1_html.find("select", id="search_page").find_all("option"))
    page_1_task = scrape_single_page(session, page_1_html, pack_names_to_ids)
    page_2_n_task = [thread_func(i) for i in range(2, num_pages+1)]
    all_pages = await asyncio.gather(page_1_task, *page_2_n_task)
    return [song for page in all_pages for song in page]


def merge_songlists(arcade_songlist, pc_songlist):
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
        song["title"].lower(),
    ))

    for i, song in enumerate(merged_songlist):
        song["id"] = i

    return merged_songlist


async def scrape_single_jacket(session, jacket_num, konami_id):
    """Downloads a single jacket and places it in the folder `jackets`."""

    jacket_path = f"./jackets/{jacket_num}.jpg"
    image_url = EAC_JACKET_PATH + konami_id
    while True:
        try:
            async with session.get(image_url, timeout=TIMEOUT) as response:
                bytes_ = await response.read()
                print(f"Scraped jacket with ID {konami_id}...")
                async with aiofiles.open(jacket_path, mode='wb') as f:
                    await f.write(bytes_)
                return
        except:
            pass
    


async def scrape_jacket_art(session, songlist, jacket_nums_to_konami_ids):
    """Downloads the jacket art to each song."""

    # if not os.path.isdir("./jackets"):
    #     os.mkdir("./jackets")
    # else:
    #     shutil.rmtree("./jackets")
    # Currently just dies.
    await asyncio.gather(*[scrape_single_jacket(session, chart["jacket"], jacket_nums_to_konami_ids[chart["jacket"]])
        for song in songlist for chart in song["charts"]])


async def request_html(session, url, params={}):
    """Takes a song page URL and returns the HTML of the page."""

    # KONMAI servers bad
    while True:
        try:
            async with session.get(url, params=params, timeout=TIMEOUT) as response:
                html = await response.text()
                result = BeautifulSoup(html, "html.parser")
                # Absurd Konmai server bug
                assert len(list(result.find_all("div"))) > 0
                return result
        except asyncio.TimeoutError:
            pass
    
    while True:
        try:
            async with session.get(url, params=params, timeout=TIMEOUT) as response:
                html = await response.text()
                result = BeautifulSoup(html, "html.parser")
                # Absurd Konmai server bug
                assert len(list(result.find_all("div"))) > 0
                return result
        except:
            print(f"Song error stuff. {url}")
            pass


if __name__ == "__main__":
    # with open("results.json", encoding="utf-8") as f:
    #     results = json.load(f)
    # with open("website_results.json", encoding="utf-8") as f:
    #     web_results = json.load(f)
    # web_titles = set(song["title"] for song in web_results)
    # new_titles = set(song["title"] for song in results["songs"])
    # for title in web_titles:
    #     if title not in new_titles:
    #         print(title)
    # 5/0

    # Currently KONAMI servers reject my ass for being too fast. Fix this.


    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

    