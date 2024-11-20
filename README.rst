.. title:: introduction

============
UFC Scraper
============

.. image:: https://github.com/balaustrada/ufcscraper/actions/workflows/python--app.yml/badge.svg
   :target: https://github.com/balaustrada/ufcscraper/actions/workflows/python--app.yml

.. image:: https://readthedocs.org/projects/ufc-scraper/badge/?version=latest
   :target: https://ufc-scraper.readthedocs.io/en/latest/

.. image:: https://coveralls.io/repos/github/balaustrada/ufcscraper/badge.svg?branch=main
    :target: https://coveralls.io/github/balaustrada/ufcscraper?branch=main

.. image:: https://github.com/balaustrada/ufcscraper/actions/workflows/mypy.yml/badge.svg
   :target: https://github.com/balaustrada/ufcscraper/actions/workflows/mypy.yml

This project is a data scraper designed to collect and process fight statistics and betting odds for UFC events. It is composed of two parts:

1. **Scraping UFC Statistics**: Data from events, fights, and fighters is scraped from `UFC stats <http://ufcstats.com/>`_ and stored in CSV format.

2. **Scraping Betting Odds**: Betting odds for UFC fights are scraped from `BestFightOdds <https://bestifghtodds.com/>`_ and matched to the correct fighters.

The data model for the UFC statistics part can be found in `UFC statistics model <tables/ufcstats_tables.html>`_ while the one for BestFightOdds odds can be found in `BestFightOdds model <tables/bestfightodds_tables.html>`_.

Installation
==============

After cloning the environment:

.. code-block:: shell

    git clone https://github.com/balaustrada/ufcscraper.git

The code can be easily installed through participants

.. code-block:: shell
    
    pip install .


Usage
======

Once installed, there are two entry points to be used for scraping data:

* ``ufcscraper_scrape_ufcstats_data``: Scrape information from UFC stats.
* ``ufcscraper_scrape_bestfightodds_data``: Scrape information from BestFightOdds.

Credits
========

The methods for scraping UFC stats data are derived from the ones in `https://github.com/remypereira99/UFC-Web-Scraping <https://github.com/remypereira99/UFC-Web-Scraping>`_.

Disclaimer
===========

This repository is for educational purposes only. The author does not promote, encourage, support, or excite any illegal activity or hacking without written permission. The author and this repository are not responsible for any misuse of the information provided.

The software and scripts provided in this repository should only be used for educational purposes. The author cannot be held responsible for any misuse by users.

The author is not responsible for any direct or indirect damage caused due to the use of the code provided in this repository. All the information provided here is for educational purposes only.
