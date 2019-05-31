from requests_html import HTML
from string import ascii_uppercase
import copy
import json
from tqdm import tqdm
import grequests
import utils


BASE_LINK = 'http://www.howstat.com/cricket/Statistics/Players/PlayerList.asp?Country=ALL&Group'
PLAYER_ODI_LINK = 'http://www.howstat.com/cricket/Statistics/Players/PlayerYears_ODI.asp'
ALL_YEARS = {}
BATCH_SIZE = 200


def get_player_match_data(session, player_code):
    player_data = session.get(f'{PLAYER_ODI_LINK}?{player_code}').html
    player_data_table = player_data.find('table')[5]
    player_match_years = player_data_table.find('tr')[2:-1]
    player_cummulative_runs = 0
    player_cummulative_matches = 0
    player_all_years_data = []

    for year in player_match_years:
        year_dict = {}
        year_data = year.find('td')
        year_dict['year'] = int(year_data[0].text.strip())
        year_dict['year_matches'] = int(year_data[1].text.strip())
        player_cummulative_matches += year_dict['year_matches']
        year_dict['year_matches_cummulative'] = player_cummulative_matches
        year_dict['year_runs'] = int(year_data[8].text.strip())
        player_cummulative_runs += year_dict['year_runs']
        year_dict['year_runs_cummulative'] = player_cummulative_runs
        
        player_all_years_data.append(year_dict)
    
    return player_all_years_data


def get_players(session):
    all_players = []
    for x in tqdm(ascii_uppercase):
        player_list = session.get(f'{BASE_LINK}={x}').html
        player_table = player_list.find('table')[4].find('table')[2]
        player_trs = player_table.find('tr')[2:-1]
        for tr in player_trs:
            player_tds = tr.find('td')
            if player_tds[4].text.strip():  # In some cases, the player has never played an ODI match, so avoid increasing network hits in that case
                player_code = player_tds[0].find('a')[0].attrs['href'].split('?')[-1]
                all_players.append(player_code)
            else:
                continue
    
    return all_players


def main():
    session = HTML()
    print('Collecting all players who have ever played ODI matches')
    all_players = get_players(session)
    print(f'Number of players found: {len(all_players)}')
    all_players_data = []
    print('Processing individual player data')
    with tqdm(total=len(all_players)) as progress_bar:
        for batch in utils.divide_list(all_players):
            player_data_responses = (grequests.get(f'{PLAYER_ODI_LINK}?{player_code}') for player_code in batch)
            player = all_players.pop(0)
            player_final = copy.deepcopy(player)  # Necessary as it's probably a bad practice to edit the same dict while iterating through it
            del player_final['player_code']
            # This stores a lot of data in the memory at runtime, but it's a tradeoff,
            # considering the data isn't big enough to need to put on disk and it increases overall speed
            player_final['player_data'] = get_player_match_data(session, player['player_code'])
            all_players_data.append(player_final)
            progress_bar.update(1)
    print('Dumping JSON to disk')
    with open('all_player_data.json', 'w') as player_data_file:
        player_data_file.write(json.dumps(all_players_data, indent=4))
    print('All done!')

main()
