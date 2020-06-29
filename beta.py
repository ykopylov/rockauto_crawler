from bs4 import BeautifulSoup
import requests
import time
import random
import sqlite3

INSERT_INTO = " INSERT INTO car_catalog (`brand`, `year`, `model`, `volume`, `группа запчастей`, `подгруппы запчастей`, `number`, `applied_models`, `price`, `part_url`) values('%s','%s' , '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s') "

mydb = sqlite3.connect('result.sql')

mycursor = mydb.cursor()
try:
    print('Creating Database')
    mycursor.execute("CREATE DATABASE 'result.sql' ")
    print('database created')
    mydb.commit()
except:
    print('DAtabase already exists')
    pass
mycursor.execute("CREATE TABLE IF NOT EXISTS `car_catalog` ("
                 "  `brand` varchar(255) NOT NULL,"
                 "  `year` varchar(255) NOT NULL,"
                 "  `model` varchar(255) NOT NULL,"
                 "  `volume` varchar(255) NOT NULL,"
                 "  `группа запчастей` varchar(255) NOT NULL,"
                 "  `подгруппы запчастей` varchar(255) NOT NULL,"
                 "  `number` varchar(255) NOT NULL,"
                 "  `applied_models` varchar(255) NOT NULL,"
                 "  `price` varchar(255) NOT NULL,"
                 "  `part_url` varchar(255) DEFAULT NULL)")

mydb.commit()


class Getter:
    def __init__(self):
        with open('proxies.txt', 'r') as f:
            self.proxies = f.read().split('\n')
            self.proxies = [i.split(' ')[0].strip() for i in self.proxies]
        with open('user_agents.txt', 'r') as f:
            self.useragents = f.read().split('\n')
        self.proxy = random.choice(self.proxies)
        self.useragent = random.choice(self.useragents)

    def get_html(self, url):
        try:
            html = requests.get(url, proxies={'http': 'http://' + self.proxy,
                                              'https': 'https://' + self.proxy},
                                headers={'User-Agent': self.useragent})
            if str(html.status_code).startswith('2'):
                return html.text
            else:
                print('Unknown error. Try again and then exit')
                self.change_proxy()
                self.change_useragent()
                time.sleep(5)
                html = requests.get(url, proxies={'http': 'http://' + self.proxy,
                                                  'https': 'https://' + self.proxy},
                                    headers={'User-Agent': self.useragent})
                if str(html.status_code).startswith('2'):
                    return html.text
        except requests.exceptions.Timeout as e:
            self.change_proxy()
            self.change_useragent()
            print(e)
            print('Changing Proxy', self.proxy)
            return self.get_html(url)
        except requests.exceptions.RequestException as e:
            self.change_proxy()
            self.change_useragent()
            print(e)
            print('Changing Proxy', self.proxy)
            return self.get_html(url)

    def change_proxy(self):
        if len(self.proxies) > 30:
            self.proxies.remove(self.proxy)
        self.proxy = random.choice(self.proxies)

    def change_useragent(self):
        self.useragent = random.choice(self.useragents)


class AutoScrapper():
    def __init__(self, soup):
        self.soup = soup
        self.getter = Getter()

    def get_brand_name_and_link(self, brand):
        last_div = brand.find('div')
        next_reveal = last_div.find('a', class_='navlabellink nvoffset nnormal')
        # 2 classes of cars
        if next_reveal is None:
            next_reveal = last_div.find('a', class_='navlabellink nvoffset nimportant')
        if next_reveal is None:
            next_reveal = last_div.find('a', class_='navlabellink nvoffset nreversevideo')
        brand_name = next_reveal.text
        next_link = next_reveal['href']
        return (brand_name, 'https://www.rockauto.com' + next_link)

    def get_all_brands_dict(self):
        all_brands = self.soup.find('div', id="treeroot[catalog]").find_all('div', class_='ranavnode')
        brand_links = dict()
        for brand in all_brands:
            brand_name, next_link = self.get_brand_name_and_link(brand)
            brand_links[brand_name] = next_link
        return brand_links

    def get_node(self, url, brand='982cc L4'):
        html = self.getter.get_html(url)
        soup = BeautifulSoup(html, 'lxml')
        tag = soup.find(text=brand)
        # back to the upper node
        soup = tag.parent.parent.parent.parent.parent.parent.parent.parent
        nchildren = soup.find('div', class_='nchildren')
        nchildren.find_all('div', class_='ranavnode')
        res_dct = {}
        if nchildren:
            for child in nchildren:
                year, next_link = self.get_brand_name_and_link(child)
                res_dct[year] = next_link
        return res_dct

    def get_price(self, url):
        html = self.getter.get_html(url)
        soup = BeautifulSoup(html, 'lxml')
        # soup = self.soup
        table = soup.find('div', class_='listing-container-border')
        altrows = table.find_all('tbody', class_='listing-inner altrow-a-0')
        if not altrows:
            altrows = []
        altrows += table.find_all('tbody', class_='listing-inner altrow-a-1')

        res_price, res_num, res_part = [], [], []
        for row in altrows:
            try:
                price = row.find_all('span', class_='ra-formatted-amount listing-price listing-amount-bold')[0].text
            except IndexError:
                price = ''
            try:
                number = row.find_all('span', title="Replaces these Alternate/ OE Part Numbers")[0].text
            except IndexError:
                number = ''
            # applied_details = table.find_all('span', class_='listing-final-partnumber  as-link-if-js buyers-guide-color')
            try:
                applied_details = row.find_all('span', title="Buyer's Guide")[0].text
                if applied_details is None:
                    applied_details = table.find_all('span',
                                                     class_='listing-final-partnumber  as-link-if-js buyers-guide-color')[
                        0].text
            except IndexError:
                applied_details = ''

            if price != '' and number != '' and applied_details != '':
                res_price.append(price)
                res_num.append(number)
                res_part.append(applied_details)
        return res_price, res_num, res_part

    def get_data(self):
        # dict --> brand - href
        brand_links = self.get_all_brands_dict()
        for brand in brand_links:
            print('Parsing', brand)
            try:
                years_dct = self.get_node(brand_links[brand], brand)
            except:
                print('Unknown Error, skipping', brand_links[brand])
                continue
            for year in years_dct:
                print('  Parsing', year)
                try:
                    models = self.get_node(years_dct[year], year)
                except:
                    print('Unknown Error, skipping', years_dct[year])
                    continue
                for model in models:
                    print('\tParsing', model)
                    try:
                        volumes = self.get_node(models[model], model)
                    except:
                        print('Unknown Error, skipping', models[model])
                        continue
                    for volume in volumes:
                        print('\t  Parsing', volume)
                        try:
                            parts = self.get_node(volumes[volume], volume)
                        except:
                            print('Unknown Error, skipping', volumes[volume])
                            continue
                        for part in parts:
                            print('        Parsing', part)
                            try:
                                ex_parts = self.get_node(parts[part], part)
                            except:
                                print('Unknown Error, skipping', parts[part])
                                continue
                            if ex_parts:
                                for ex in ex_parts:
                                    print('        Parsing', ex)
                                    try:
                                        price, number, applied_details = self.get_price(ex_parts[ex])
                                    except:
                                        print('Unknown error', ex_parts[ex])
                                        continue
                                    for i in range(len(price)):
                                        res = {'brand': brand,
                                               'year': year,
                                               'model': model,
                                               'volume': volume,
                                               'группа запчастей': part,
                                               'подгруппы запчастей': ex,
                                               'number': number[i],
                                               'applied_model': applied_details[i],
                                               'price': price[i],
                                               'part_url': ex_parts[ex]}
                                        print(res)
                                        try:
                                            with open('res.csv', 'a') as f:
                                                for val in res.values():
                                                    f.write(val + ';')
                                                f.write('\n')
                                        except:
                                            print('Failed to save into CSV')
                                        try:
                                            mycursor.execute(INSERT_INTO % tuple(res.values()))
                                            mydb.commit()
                                        except:
                                            print('Failed to write into Database')
                        print('\nPROCESSED PARTS\n')
                    print('\nPROCESSED VOLUMES\n')
                print('\nPROCESSED MODELS\n')
            print('\nPROCESSED YEARS\n')
        print('\n\nPROCESSED', brand, '\n\n')

    # div listing-container-border
    # volumes = self.get_node(brand_links['ABARTH'], 'Brake & Wheel Hub')
    # models = self.get_models()
    # price, number, applied_details = self.get_price('some url')
    # print(price, number, applied_details)


if __name__ == '__main__':
    # with open('years.html', 'r') as f:
    #     data = f.read()
    # print(data)
    data = Getter().get_html('https://www.rockauto.com')
    AutoScrapper(BeautifulSoup(data, 'lxml')).get_data()
    mycursor.close()
    mydb.close()
