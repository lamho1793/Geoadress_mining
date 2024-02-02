# This is a sample Python script.
from multiprocessing import Process, Queue
import aiohttp 
import asyncio
import os, time, random,timeit
import requests
import pandas as pd
import csv

def write_to_queue(q):
    main_data = pd.read_csv('ADDRESS_TEST.csv', header=0)
    for index, row in main_data.iterrows():
        # row_number = row['row_number']
        # user_id = row['user_id']
        # addr_dtl = row['addr_dtl']
        row_number = row[0]
        user_id = row[1]
        addr_dtl = ' '.join(row.astype('str').tolist()).replace('nan ','').replace(' nan','')
        q.put((row_number, user_id, addr_dtl))


def read_from_queue_and_process(q, n):
    with open('address_' + n + '.csv', 'w+', newline='', encoding="utf-8") as csvwitfile:
        writer = csv.writer(csvwitfile, delimiter='|')
        while True:
            try:
                row = q.get(True,10)
            except:
                break
            row_number = row[0]
            user_id = row[1]
            addr_dtl = row[2]
            result = process_address(addr_dtl)
            if result is not None:
                result.insert(0, user_id)
                result.insert(0, row_number)
                writer.writerow(result)
                print(result)
            else:
                writer.writerow(['NULL']*14)


def process_address(address: object) -> object:
    pload = {'q': address}
    headers = {'Accept': "application/json"}
    r = requests.get("https://www.als.ogcio.gov.hk/lookup", params=pload, headers=headers)
    if r.status_code == requests.codes.ok:
        text = r.json()['SuggestedAddress'][0]
        # process English Address Part
        # process Chinese Address Part
        building_name = None
        if 'BuildingName' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
            building_name = text['Address']['PremisesAddress']['ChiPremisesAddress']['BuildingName']
        chi_block = None
        block_no = None
        block_descriptor = None
        if 'ChiBlock' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
            chi_block = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiBlock'];
            if 'BlockNo' in text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiBlock']:
                block_no = chi_block['BlockNo']
            if 'BlockDescriptor' in text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiBlock']:
                block_descriptor = chi_block['BlockDescriptor']
        chi_estate = None
        chi_phase = None
        phase_name = None
        estate_name = None
        if 'ChiEstate' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
            chi_estate = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiEstate']
            if 'ChiPhase' in chi_estate:
                chi_phase = chi_estate['ChiPhase']
                if 'PhaseName' in chi_phase:
                    phase_name = chi_phase['PhaseName']
            if 'EstateName' in chi_estate:
                estate_name = chi_estate['EstateName']
        chi_village = None
        chi_street = None
        location_name = None
        village_name = None
        street_name = None
        building_no_from = None
        if 'ChiVillage' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
            chi_village = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiVillage']
            if 'LocationName' in chi_village:
                location_name = chi_village['LocationName']
            if 'VillageName' in chi_village:
                street_name = chi_village['VillageName']
            if 'BuildingNoFrom' in chi_village:
                building_no_from = chi_village['BuildingNoFrom']

        if 'ChiStreet' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
            chi_street = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiStreet']
            if 'LocationName' in chi_street:
                location_name = chi_street['LocationName']
            if 'StreetName' in chi_street:
                street_name = chi_street['StreetName']
            if 'BuildingNoFrom' in chi_street:
                building_no_from = chi_street['BuildingNoFrom']
        dc_district = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiDistrict']['DcDistrict']
        region = text['Address']['PremisesAddress']['ChiPremisesAddress']['Region']

        # process GeoAddress
        geo_address = text['Address']['PremisesAddress']['GeoAddress']
        # process GeospatialInformation
        latitude = text['Address']['PremisesAddress']['GeospatialInformation']['Latitude']
        longitude = text['Address']['PremisesAddress']['GeospatialInformation']['Longitude']
        # process ValidationInformation
        score = text['ValidationInformation']['Score']
        return [latitude, longitude, score, geo_address, dc_district, region, building_name, block_no, block_descriptor, location_name, street_name, building_no_from, phase_name, estate_name]
    else:
        return None

async def process_address_op2(address: object) -> object:
    pload = {'q': address}
    headers = {'Accept': "application/json"}
    async with aiohttp.ClientSession() as session:
        async with session.get ("https://www.als.ogcio.gov.hk/lookup", params=pload, headers=headers) as r:
        # r = await session.get("https://www.als.ogcio.gov.hk/lookup", params=pload, headers=headers)
            if r.status_code == requests.codes.ok:
                text = r.json()['SuggestedAddress'][0]
                # process English Address Part
                # process Chinese Address Part
                building_name = None
                if 'BuildingName' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
                    building_name = text['Address']['PremisesAddress']['ChiPremisesAddress']['BuildingName']
                chi_block = None
                block_no = None
                block_descriptor = None
                if 'ChiBlock' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
                    chi_block = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiBlock'];
                    if 'BlockNo' in text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiBlock']:
                        block_no = chi_block['BlockNo']
                    if 'BlockDescriptor' in text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiBlock']:
                        block_descriptor = chi_block['BlockDescriptor']
                chi_estate = None
                chi_phase = None
                phase_name = None
                estate_name = None
                if 'ChiEstate' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
                    chi_estate = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiEstate']
                    if 'ChiPhase' in chi_estate:
                        chi_phase = chi_estate['ChiPhase']
                        if 'PhaseName' in chi_phase:
                            phase_name = chi_phase['PhaseName']
                    if 'EstateName' in chi_estate:
                        estate_name = chi_estate['EstateName']
                chi_village = None
                chi_street = None
                location_name = None
                village_name = None
                street_name = None
                building_no_from = None
                if 'ChiVillage' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
                    chi_village = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiVillage']
                    if 'LocationName' in chi_village:
                        location_name = chi_village['LocationName']
                    if 'VillageName' in chi_village:
                        street_name = chi_village['VillageName']
                    if 'BuildingNoFrom' in chi_village:
                        building_no_from = chi_village['BuildingNoFrom']

                if 'ChiStreet' in text['Address']['PremisesAddress']['ChiPremisesAddress']:
                    chi_street = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiStreet']
                    if 'LocationName' in chi_street:
                        location_name = chi_street['LocationName']
                    if 'StreetName' in chi_street:
                        street_name = chi_street['StreetName']
                    if 'BuildingNoFrom' in chi_street:
                        building_no_from = chi_street['BuildingNoFrom']
                dc_district = text['Address']['PremisesAddress']['ChiPremisesAddress']['ChiDistrict']['DcDistrict']
                region = text['Address']['PremisesAddress']['ChiPremisesAddress']['Region']

                # process GeoAddress
                geo_address = text['Address']['PremisesAddress']['GeoAddress']
                # process GeospatialInformation
                latitude = text['Address']['PremisesAddress']['GeospatialInformation']['Latitude']
                longitude = text['Address']['PremisesAddress']['GeospatialInformation']['Longitude']
                # process ValidationInformation
                score = text['ValidationInformation']['Score']
                return await [latitude, longitude, score, geo_address, dc_district, region, building_name, block_no, block_descriptor, location_name, street_name, building_no_from, phase_name, estate_name]
            else:
                return await None


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def main():
    # 父进程创建Queue，并传给各个子进程：
    q = Queue()
    pw = Process(target=write_to_queue, args=(q,))
    pr_list = []
    max = 3
    for n in range(0, max):
        pr = Process(target=read_from_queue_and_process, args=(q, 'process' + str(n),))
        pr_list.append(pr)
    # 启动子进程pw，写入:
    pw.start()
    # 启动子进程pr，读取:
    for pr in pr_list:
        pr.start()
    # 等待pw结束:
    pw.join()
    # pr进程里是死循环，无法等待其结束，只能强行终止:
    # for pr in pr_list:
    #      pr.terminate()
    

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = timeit.default_timer()
    main()
    stop = timeit.default_timer()
    print('Time: ', stop - start) 

# See PyCharm help at https://www.jetbrains.com/help/pycharm/