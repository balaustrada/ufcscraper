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

This project is a data scraper designed to collect and process fight statistics and betting odds for UFC events. It is composed of two parts:

1. **Scraping UFC Statistics**: Data from events, fights, and fighters is scraped from `UFC stats <http://ufcstats.com/>`_ and stored in CSV format.

2. **Scraping Betting Odds**: Betting odds for UFC fights are scraped from `BestFightOdds <https://bestifghtodds.com/>`_ and matched to the correct fighters.

The data model for the UFC statistics part can be found `here <tables/ufcstats_tables.html>`_ while the one for BestFightOdds odds can be found `here <tables/bestfightodds_tables.html>`_.

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
