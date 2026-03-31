<div align="center">

# 📈 ChemOrbis Price Data Scraper

[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white)](https://www.selenium.dev/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Openpyxl](https://img.shields.io/badge/Openpyxl-009688?style=for-the-badge&logo=microsoftexcel&logoColor=white)](https://openpyxl.readthedocs.io/)

**Автоматизированный ETL-пайплайн для сбора и консолидации данных о ценах на полимеры с платформы ChemOrbis.**

[О проекте](#-о-проекте) · [Возможности](#-возможности) · [Установка](#-установка) · [Использование](#-использование)

---

</div>

## 📖 О проекте

Проект автоматизирует ручной процесс сбора данных о ценах на полимерное сырьё с [ChemOrbis](https://www.chemorbis.com) — ведущей платформы для мониторинга нефтехимического рынка. 

Инструмент извлекает еженедельные ценовые индексы и данные интерактивных графиков, объединяет их и формирует структурированный Excel-файл, готовый для BI-аналитики.

> [!TIP]
> **Бизнес-ценность:** Ранее ручной сбор данных занимал несколько часов в неделю. Теперь полный цикл выполняется автоматически за **~30 минут** без участия оператора.

---

## ✨ Возможности

### 🕷️ Умный веб-скрапинг
- **Надежная авторизация** с обработкой всплывающих окон, cookie-баннеров и Shadow DOM.
- **Price Index** — автоматическая выгрузка еженедельных отчётов (котировки Low / Avg / High).
- **Price Wizard** — экспорт исторических данных временных рядов (CSV) прямо из интерактивных графиков.
- **Механизм самовосстановления (Retries)** — настраиваемое количество попыток с таймаутами для обхода временных блокировок.

### 📊 Обработка данных
- **Консолидация** — объединение данных из разных источников в единый датафрейм.
- **Очистка и стандартизация** — автоматический маппинг валют, единиц измерения и названий котировок.
- **Умный экспорт** — генерация чистого, отформатированного Excel-файла с правильными типами данных (даты, числа).

### ⚙️ Инфраструктура
- Гибкая настройка через `config.yaml` (URL, таймауты, маппинги).
- Безопасное хранение учетных данных в `.env`.
- CLI-интерфейс для запуска отдельных этапов (только скрапинг, только обработка).
- Детальное логирование процессов в консоль и файлы.

---

## 🏗️ Архитектура (ETL Pipeline)

```mermaid
graph TD
    Config["config.yaml<br>.env"] --> Main
    Main{"main.py<br>(Оркестратор)"} --> Auth["auth.py<br>Авторизация"]
    
    Auth --> S_Index["scraper_price_index.py<br>(Еженедельные отчеты)"]
    Auth --> S_Wizard["scraper_price_wizard.py<br>(Графики цен)"]
    
    S_Index --> RawData[("Сырые данные<br>(Excel / CSV)")]
    S_Wizard --> RawData
    
    RawData --> Processor["data_processor.py<br>(Очистка и объединение)"]
    Processor --> Formatter["excel_formatter.py<br>(Стилизация и типы)"]
    Formatter --> Final[("Итоговый отчет<br>chemorbis_consolidated.xlsx")]
    
    style Main fill:#3776AB,color:#fff
    style Final fill:#43B02A,color:#fff
