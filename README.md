# PyFolio Core

![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)

**PyFolio Core** is a robust, open-source financial portfolio tracking engine built with Python. Designed as a backend kernel, it fetches real-time asset data from public APIs and stores it locally.

It is built with **Clean Architecture** principles to serve as a foundation for future GUI (PySide) or Web applications.

## Features

* **Clean Architecture:** Built with SOLID principles (Interface segregation, Separation of concerns).
* **Dual Engine:**
    * **Stocks:** Fetches daily closing prices from TradingView (via `tvdatafeed`).
    * **Funds:** Fetches daily fund prices from TEFAS (via `tefas-crawler`).
* **Privacy First:** All data is stored in a local SQLite database (`/data` folder).
* **Modular Design:** Interface-based architecture allowing easy swap of data providers.
* **Type Safe:** Uses Enums and Type Hinting for reliability.
* **Zero-Scraping:** Uses public APIs instead of HTML scraping for stability and compliance.

## Roadmap

* [x] **Core:** Basic asset tracking & price updating.
* [ ] **History:** Daily closing price logging (Time Series data).
* [ ] **Analytics:** ROI, volatility, and distribution charts.
* [ ] **UI:** Desktop GUI development with **PySide6/Qt**.

## Architecture

The project follows a modular structure:
* `core/`: Contains the business logic and data fetchers (`StockService`, `FundService`).
* `database/`: Handles SQLite connections and schema migrations.
* `ui/`: (Planned) User Interface layer.

## Installation

1.  **Clone & Install:**
    ```bash
    git clone https://github.com/DeponesLabs/PyFolio-Core.git
    pip install -r requirements.txt
    ```

2.  **Initialize Database:**
    The system will automatically generate `data/portfolio.db` on the first run.

3.  **Run:**
    ```bash
    python main.py
    ```

## ⚠️ Legal Disclaimer

This project is for **educational purposes only**.

* The data is retrieved from public third-party libraries (`tvdatafeed`, `tefas-crawler`).
* The developer is not responsible for any financial losses or data inaccuracies.
* Use this tool responsibly and in accordance with the data providers' terms of service.