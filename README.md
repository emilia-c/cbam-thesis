# Master Thesis 2025

This repository contains all the code and data related to the Towards an Semi-Automated Scenario Analysis: Exploring Carbon Data Futures for the Carbon Border Adjustment Mechanism thesis. 

## Project Overview

The project focuses on building a comprehensive corpus of stakeholder and EU sources for analyzing the Carbon Border Adjustment Mechanism (CBAM). Additionally, it develops a method inspired by the Scenario Acceleration through Automated Modelling (SAAM) approach, as presented by Davis, C. (2022). SAAM is described in *Scenario Acceleration Through Automated Modelling: A Method and System for Creating Traceable Quantitative Future Scenarios Based on FCM System Modeling and Natural Language Processing*. Doctor of Philosophy in Technology Management. Portland State University. Available at: [https://doi.org/10.15760/etd.7878](https://doi.org/10.15760/etd.7878). The method in this project further incorporates the [InCognitive](https://github.com/ThemisKoutsellis/InCognitive) tool for the FCM stage. 

OBS: Raw url downloads can be requested. 

## Corpus Building

### 1. Stakeholder Sources

#### 1.1 List of Stakeholders

* **EC/EP CBAM Meetings**

  * EC CBAM meetings from LobbyMap, stored in `eu_commission_cbam_meetings.xlsx`
  * EP CBAM meetings from cbam\_meeting\_information, extracted from meeting records of MEPs, stored in `aggregated_cbam_meeting_data.xlsx`
  * Merged data in `RAW_merged_cbam_meetings.xlsx`
* **Public Consultation Responses**

  * Stakeholders who responded to three public consultation calls, stored in `all_hys_orgs.xlsx`
* **Optimized Stakeholder Names for DuckDuckGo**

  * Stakeholder names from both meetings and public consultation responses were optimized for search using DuckDuckGo.
  * Final list of optimized names stored in `organisation_titles.xlsx`

#### 1.2 Building Stakeholder Corpus

##### 1.2.1 Stakeholder Web Scrape

* All scripts are part of the `stakeholder_data_extraction_pipeline`
* Extracts top 20 results via DuckDuckGo for the query (stakeholder + CBAM future)
* Downloads results locally and stores them in an SQLite database
* Filters out social media links and splits content into HTML/PDF
* Extracts HTML text using [Trafilatura](https://github.com/adbar/trafilatura) and PDF text using [DeepDoctection](https://github.com/deepdoctection/deepdoctection)
* Processed data stored in `processed_data`

##### 1.2.2 Public Consultation Response

* Uses [HaveYourSay](https://github.com/ghxm/haveyoursay) to obtain PDFs with stakeholder responses and organization titles from CBAM-related initiatives
* Extracted stakeholder responses via `scrape_cbam_feedback.py`
* PDF text extracted via `extract_feedback_text.py`, saved in `combined_hys.json`

### 2. EU Sources

* **Committee Documents**

  * PDFs sourced from the [EP E-Meeting Archive](https://emeeting.europarl.europa.eu/emeeting/committee/en/archives) for CBAM committees
  * Extracted using `committee_pdf_extraction.py`, saved in `committee_meetings_data.json`
* **EP CBAM Communication**

  * Extracted via `EP_press_release.py`, saved in `EP_CBAM_articles.json`
* **EP Political Group Communication**

  * Extracted manually from EP group websites via CBAM keyword search, saved in `group_press_releases.json`
* **EC CBAM Communication**

  * Extracted via `EC_scrape_press_release.py`, saved in `ENGLISH_EC_CBAM_articles.json` after manual cleaning
* **CBAM Legislation**

  * Sourced from [EU CBAM Legislation](https://taxation-customs.ec.europa.eu/carbon-border-adjustment-mechanism_en)
  * Extracted via `ec_leg_pdf_extraction.py`, stored in `legislation_data.json`

## Topic Modelling

* Performed on both stakeholder and EU corpora (contains only EP/EC CBAM communication and EP political communication documents)
* Scripts: `EU_lda_modelling.ipynb`, `stakeholder_lda_modelling.ipynb`

## QA Modelling

* Performed using BERT, run in Google Notebooks due to memory requirements
* Scripts: `qa_saam.ipynb`, `scenario_prep.ipynb` (prepares data for Incognitive)

## Fuzzy Cognitive Map via In-Cognitive

* Run FCM simulations via In-Cognitive 
* Scripts: `run_incognitive.py`, `scenario_visualization.ipynb`
