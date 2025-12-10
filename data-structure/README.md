# Hurtownia Danych Wheelie - Dokumentacja

## 📋 Spis treści
1. [Przegląd architektury](#przegląd-architektury)
2. [Tabele faktów](#tabele-faktów)
3. [Wymiary](#wymiary)
4. [Tabela pomostowa](#tabela-pomostowa)
5. [Współdzielenie wymiarów](#współdzielenie-wymiarów)
6. [Mapowanie pytań biznesowych](#mapowanie-pytań-biznesowych)
7. [Proces ETL](#proces-etl)

---

## Przegląd architektury

### Typ architektury
**Star Schema (Schemat gwiazdy)** z tabelą pomostową

### Struktura
- **2 tabele faktów:** `fact_rental`, `fact_service`
- **7 wymiarów:** `dim_customer`, `dim_car`, `dim_staff`, `dim_store`, `dim_payment`, `dim_date`, `dim_equipment`
- **1 tabela pomostowa:** `bridge_car_equipment`

### Diagram konceptualny
```
                    dim_date (współdzielony)
                      ↓     ↓
    dim_customer → fact_rental ← dim_car ← bridge_car_equipment → dim_equipment
    dim_staff    →     ↓        ↗  ↓
    dim_payment  →   dim_store   ↓
                                fact_service
```

---

## Tabele faktów

### 1. fact_rental (Fakty wypożyczeń)

**Ziarno:** Jeden wiersz = jedno wypożyczenie

**Przeznaczenie:**
Główna tabela transakcyjna przechowująca wszystkie zdarzenia wypożyczeń samochodów.

**Klucze obce:**
- `customer_key` → `dim_customer` (klucz zastępczy dla SCD Type 2)
- `car_key` → `dim_car`
- `staff_key` → `dim_staff` (który pracownik obsłużył)
- `store_key` → `dim_store` (gdzie wypożyczono)
- `payment_key` → `dim_payment` (nullable - płatność może być w toku)
- `rental_date_key` → `dim_date` (data wypożyczenia)
- `return_date_key` → `dim_date` (data zwrotu, nullable dla aktywnych)

**Miary:**
- `rental_rate` - stawka wypożyczenia (przychód)
- `rental_duration_days` - długość wypożyczenia w dniach (nullable dla aktywnych)

**Obsługiwane pytania biznesowe:**
- Ile zarobiliśmy? → `SUM(rental_rate)`
- Typowa długość wypożyczenia → `AVG(rental_duration_days)`
- Ranking wypożyczalni → `GROUP BY store_key`
- Podział na sprzedawców → `GROUP BY staff_key`
- Analiza powracających klientów → `COUNT(*) BY customer_id`

---

### 2. fact_service (Fakty serwisowe)

**Ziarno:** Jeden wiersz = jedno zdarzenie serwisowe

**Przeznaczenie:**
Śledzenie wszystkich zdarzeń konserwacji i napraw samochodów w inwentarzu.

**Uzasadnienie osobnej tabeli:**
Wymiar `dim_car` zawiera zagregowane koszty serwisu (`total_service_cost`), ale brakuje szczegółów czasowych.
Bez `fact_service` nie można odpowiedzieć na pytania typu:
- "Ile serwisów wykonano w marcu 2024?"
- "Jak kształtują się koszty serwisu Q1 2024 vs Q1 2023?"
- "Trend kosztów serwisu w czasie"

**Klucze obce:**
- `car_key` → `dim_car` (który samochód)
- `service_date_key` → `dim_date` (kiedy serwisowano)
- `store_key` → `dim_store` (gdzie serwisowano)

**Miary:**
- `service_cost` - koszt zdarzenia serwisowego

**Wymiary zdegenerowane:**
- `service_type` - typ serwisu (wymiana oleju, wymiana opon, naprawa)

**Obsługiwane pytania biznesowe:**
- Koszty serwisu w czasie (KPI rok do roku)
- Liczba działań serwisowych per miesiąc
- Liczba działań per samochód/marka
- Ranking marek według kosztów serwisu

---

## Wymiary

### 1. dim_customer (Klienci)

**Typ:** SCD Type 2 (śledzenie historii zmian)

**Klucze:**
- `customer_key` - klucz zastępczy (PK), unikalny dla każdej wersji
- `customer_id` - klucz biznesowy, ten sam dla wszystkich wersji

**Atrybuty:**
- `first_name`, `last_name`, `email` - dane osobowe
- `birth_date` - data urodzenia (do obliczania wieku)
- `city`, `country` - lokalizacja klienta (skąd pochodzi)

**Pola SCD Type 2:**
- `effective_date` - początek okresu ważności wersji
- `end_date` - koniec okresu (NULL = aktualna wersja)
- `is_current` - flaga aktualnej wersji (optymalizacja zapytań)

**Przykład działania SCD Type 2:**
```
Klient #123 przeprowadza się z Warszawy do Krakowa:

| customer_key | customer_id | city     | effective_date | end_date   | is_current |
|--------------|-------------|----------|----------------|------------|------------|
| 1            | 123         | Warszawa | 2022-01-01     | 2023-06-15 | FALSE      |
| 2            | 123         | Kraków   | 2023-06-15     | NULL       | TRUE       |

Wypożyczenie z 2022-05-20 → customer_key=1 (Warszawa w momencie wypożyczenia)
Wypożyczenie z 2024-01-10 → customer_key=2 (Kraków w momencie wypożyczenia)
```

**Uwaga akademicka:**
SCD Type 2 zostało zaimplementowane jako demonstracja zaawansowanego wzorca dla celów edukacyjnych.
W praktyce, dla tych konkretnych pytań biznesowych, SCD Type 1 (tylko aktualne dane) byłby wystarczający,
ponieważ żadne pytanie nie wymaga historycznego adresu klienta w momencie wypożyczenia.

**Obsługiwane pytania:**
- Kim są nasi klienci (wiek)? → `YEAR(CURRENT_DATE) - YEAR(birth_date)`
- Skąd pochodzą klienci? → `GROUP BY city, country`
- Czy klienci wracają? → `COUNT(rentals) BY customer_id WHERE count > 2`

---

### 2. dim_car (Samochody)

**Typ:** SCD Type 1 (nadpisywanie bez historii)

**Klucze:**
- `car_key` - klucz zastępczy (PK)
- `inventory_id` - klucz biznesowy z systemu źródłowego

**Atrybuty modelu:**
- `producer` - producent/marka (Volkswagen, Toyota, BMW)
- `model` - model samochodu (Golf, Corolla, X5)

**Atrybuty instancji:**
- `production_year` - rok produkcji (do obliczania wieku auta)
- `fuel_type` - rodzaj paliwa (benzyna, diesel, elektryczny, hybrid)
- `license_plates` - tablice rejestracyjne (identyfikacja fizyczna)
- `store_key` - "domowa" lokalizacja auta (zawsze tu wraca)

**Metryki finansowe (pre-agregowane):**
- `purchase_price` - koszt zakupu samochodu
- `total_revenue` - suma wszystkich `rental_rate` z `fact_rental` dla tego auta
- `total_service_cost` - suma wszystkich `service_cost` z `fact_service` dla tego auta

**Obliczanie zyskowności:**
```
Zysk_netto = total_revenue - purchase_price - total_service_cost
```

**Dlaczego agregaty w wymiarze?**
- **Wydajność:** Szybkie KPI w Power BI bez agregacji milionów wierszy faktów
- **Elastyczność:** Szczegółowe dane nadal w tabelach faktów do drill-down
- **Aktualizacja:** Agregaty odświeżane podczas ETL (dziennie/tygodniowo)

**Obsługiwane pytania:**
- Które samochody generują najwięcej zysku? → `ORDER BY (total_revenue - purchase_price - total_service_cost) DESC`
- Najmniej wypożyczane (marki/paliwo)? → `COUNT(fact_rental) BY producer/fuel_type ASC`
- Wiek auta → `YEAR(CURRENT_DATE) - production_year`

---

### 3. dim_staff (Pracownicy)

**Typ:** SCD Type 1 (nadpisywanie bez historii)

**Klucze:**
- `staff_key` = `staff_id` (prosty klucz, nie zastępczy)

**Atrybuty:**
- `first_name`, `last_name`, `email` - dane osobowe
- `hired_date` - data zatrudnienia
- `store_key` - w którym sklepie pracuje

**Hierarchia zarządzania:**
- `manager_staff_key` - FK do `dim_staff` (samo-odniesienie)
- `manager_name` - zdenormalizowane imię managera (dla raportów)

**Przykład hierarchii:**
```
| staff_key | name         | manager_staff_key | manager_name |
|-----------|--------------|-------------------|--------------|
| 1         | Jan Kowalski | NULL              | NULL         | (Dyrektor)
| 2         | Anna Nowak   | 1                 | Jan Kowalski |
| 3         | Tomasz Lis   | 2                 | Anna Nowak   |
```

**Dlaczego potrzebny:**
Raport "Analiza operacyjna" wymaga "podziału na sprzedawców" - analiza wydajności poszczególnych pracowników.

**Obsługiwane pytania:**
- Przychód per sprzedawca → `SUM(rental_rate) BY staff_key`
- Hierarchia zarządzania → `JOIN dim_staff ON manager_staff_key`

---

### 4. dim_store (Sklepy/Wypożyczalnie)

**Typ:** SCD Type 1 (nadpisywanie)

**Klucze:**
- `store_key` = `store_id` (prosty klucz)

**Atrybuty:**
- `city` - miasto (kluczowe dla analiz geograficznych)
- `country` - kraj
- `address` - pełny adres ulicy
- `postal_code` - kod pocztowy

**Dlaczego osobna tabela?**

Wymiar `dim_store` jest **współdzielony** przez wiele encji:
- `fact_rental.store_key` - gdzie wypożyczono
- `fact_service.store_key` - gdzie serwisowano
- `dim_car.store_key` - "domowa" lokalizacja auta
- `dim_staff.store_key` - gdzie pracuje pracownik

**Zalety centralizacji:**
- Spójna definicja lokalizacji we wszystkich analizach
- Łatwe dodawanie atrybutów sklepu (region, menadżer, wielkość)
- Wsparcie hierarchii geograficznej (kraj → miasto → sklep)

**Obsługiwane pytania:**
- Jak rozkładają się wypożyczenia według miast? → `GROUP BY dim_store.city`
- Ranking wypożyczalni → `COUNT(rentals) BY store_key ORDER BY DESC`
- Gdzie powstają zaległości płatności? → `COUNT(late_payments) BY store_key`

---

### 5. dim_payment (Płatności)

**Typ:** Wymiar (nie miara w fakcie)

**Klucze:**
- `payment_key` - klucz zastępczy (PK)
- `payment_id` - klucz biznesowy

**Atrybuty:**
- `amount` - kwota płatności
- `payment_date` - kiedy zapłacono
- `payment_deadline` - termin płatności (z wypożyczenia)
- `is_late_payment` - flaga opóźnienia (pre-kalkulowana)
- `days_overdue` - dni opóźnienia (ujemne = wcześniej, dodatnie = po terminie)

**Dlaczego osobna tabela (a nie w fact_rental)?**

**Argumenty ZA:**
1. Wypożyczenie może nie mieć płatności (status: oczekująca)
2. Perspektywa biznesowa: "analiza płatności" to osobny obszar
3. Elastyczność: możliwość rozszerzenia o wiele płatności per wypożyczenie (raty)

**Obliczanie w ETL:**
```sql
is_late_payment = (payment_date > payment_deadline)
days_overdue = DATEDIFF(day, payment_deadline, payment_date)
```

**Obsługiwane pytania:**
- Analiza przeterminowanych płatności → `WHERE is_late_payment = TRUE`
- Średnie opóźnienie → `AVG(days_overdue) WHERE days_overdue > 0`
- Profil klientów z opóźnieniami → `JOIN fact_rental WHERE payment_key IN (late payments)`

---

### 6. dim_date (Daty)

**Typ:** Statyczny (pre-populowany 2018-2030)

**Klucze:**
- `date_key` - format YYYYMMDD (np. 20240315) dla efektywnych joinów
- `date` - faktyczna data

**Hierarchie czasowe:**
- `day_of_week` (1-7), `day_of_week_name` (Poniedziałek, Wtorek...)
- `day_of_month` (1-31)
- `week_of_year` (1-53)
- `month` (1-12), `month_name` (Styczeń, Luty...)
- `quarter` (1-4)
- `year` (2018-2030)
- `is_weekend` - czy weekend (sobota/niedziela)

**Flagi COVID-19 (3 boolean):**
- `is_pre_covid` - TRUE dla dat < 2020-03-01
- `is_covid` - TRUE dla 2020-03-01 do 2022-06-30
- `is_post_covid` - TRUE dla dat > 2022-06-30

**Definicje okresów COVID:**
```
Pre-COVID:  do 29.02.2020 (przed pandemią)
COVID:      01.03.2020 - 30.06.2022 (w trakcie pandemii)
Post-COVID: od 01.07.2022 (po pandemii)
```

**Dlaczego 3 flagi zamiast 1 VARCHAR?**
- Szybsze filtrowanie (boolean vs string)
- Proste zapytania w Power BI: `WHERE is_covid = TRUE`
- Efektywniejsze indeksy

**Wymiar współdzielony:**
`dim_date` jest używany przez:
- `fact_rental.rental_date_key` - data wypożyczenia
- `fact_rental.return_date_key` - data zwrotu
- `fact_service.service_date_key` - data serwisu

**Obsługiwane pytania:**
- Wypożyczenia przed/w trakcie/po COVID → `GROUP BY is_pre_covid/is_covid/is_post_covid`
- Trendy czasowe → `GROUP BY year, month, quarter`
- Sezonowość → `GROUP BY month`
- Wzorce weekend vs dzień powszedni → `GROUP BY is_weekend`

---

### 7. dim_equipment (Wyposażenie)

**Typ:** Statyczny słownik

**Klucze:**
- `equipment_key` - klucz zastępczy (PK)
- `equipment_id` - klucz biznesowy

**Atrybuty:**
- `name` - nazwa wyposażenia (GPS, Skórzane fotele, Szyberdach)
- `type` - kategoria (Bezpieczeństwo, Komfort, Technologia)

**Przykłady:**
```
| equipment_id | name                | type         |
|--------------|---------------------|--------------|
| 1            | Nawigacja GPS       | Technologia  |
| 2            | Skórzane fotele     | Komfort      |
| 3            | Szyberdach          | Komfort      |
| 4            | Kamera cofania      | Bezpieczeństwo|
| 5            | Tempomat            | Technologia  |
```

**Relacja many-to-many:**
- Jedno auto ma wiele wyposażenia
- Jedno wyposażenie występuje w wielu autach
- Rozwiązane przez `bridge_car_equipment`

**Obsługiwane pytania:**
- Najpopularniejsze wyposażenie → `COUNT(rentals) BY equipment`
- Czy zmienia się wybór wyposażenia (COVID)? → `COUNT BY equipment, covid_period`
- Najmniej wypożyczane (wyposażenie) → `COUNT(rentals) BY equipment ASC`

---

## Tabela pomostowa

### bridge_car_equipment

**Przeznaczenie:**
Rozwiązuje relację many-to-many między `dim_car` a `dim_equipment`.

**Struktura:**
- `car_key` - FK do `dim_car`
- `equipment_key` - FK do `dim_equipment`
- Composite PK: (`car_key`, `equipment_key`)

**Przykład danych:**
```
| car_key | equipment_key | (znaczenie)                    |
|---------|---------------|--------------------------------|
| 1       | 10            | Auto #1 ma GPS                 |
| 1       | 11            | Auto #1 ma Szyberdach          |
| 1       | 12            | Auto #1 ma Skórzane fotele     |
| 2       | 10            | Auto #2 ma GPS                 |
| 2       | 13            | Auto #2 ma Kamerę cofania      |
```

**Wzorce zapytań:**

1. **Znaleźć wszystkie auta z GPS:**
```sql
SELECT DISTINCT c.*
FROM dim_car c
JOIN bridge_car_equipment b ON c.car_key = b.car_key
JOIN dim_equipment e ON b.equipment_key = e.equipment_key
WHERE e.name = 'GPS'
```

2. **Power BI - policzyć unikalne auta z GPS:**
```DAX
Cars_With_GPS =
  CALCULATE(
    DISTINCTCOUNT(bridge_car_equipment[car_key]),
    dim_equipment[name] = "GPS"
  )
```

3. **Najpopularniejsze wyposażenie:**
```sql
SELECT e.name, COUNT(DISTINCT b.car_key) as car_count
FROM dim_equipment e
JOIN bridge_car_equipment b ON e.equipment_key = b.equipment_key
GROUP BY e.name
ORDER BY car_count DESC
```

**Uwaga o weight_factor:**
Oryginalny projekt zawierał pole `weight_factor` (1 / liczba_wyposażenia_w_aucie) do alokacji miar.
Zostało usunięte dla uproszczenia, ponieważ Power BI natywnie obsługuje distinct counting przez `DISTINCTCOUNT()`.

---

## Współdzielenie wymiarów

### Koncepcja
W architekturze Kimball (Star Schema), wymiary są **współdzielone** między tabelami faktów.
To kluczowa zaleta - spójne definicje i możliwość cross-fact analysis.

### Współdzielone wymiary w tej hurtowni:

#### 1. dim_date (najczęściej współdzielony)
Używany przez:
- `fact_rental.rental_date_key` - data wypożyczenia
- `fact_rental.return_date_key` - data zwrotu
- `fact_service.service_date_key` - data serwisu

**Korzyści:**
- Spójna definicja daty we wszystkich analizach
- Możliwość porównania wypożyczeń i serwisów w tym samym okresie
- Jedna tabela dla wszystkich analiz czasowych

**Przykład cross-fact query:**
```sql
-- Wypożyczenia vs serwisy w tym samym miesiącu:
SELECT
  d.year, d.month,
  COUNT(DISTINCT r.rental_key) as rentals,
  COUNT(DISTINCT s.service_key) as services
FROM dim_date d
LEFT JOIN fact_rental r ON d.date_key = r.rental_date_key
LEFT JOIN fact_service s ON d.date_key = s.service_date_key
GROUP BY d.year, d.month
```

#### 2. dim_car
Używany przez:
- `fact_rental.car_key` - które auto wypożyczono
- `fact_service.car_key` - które auto serwisowano
- `bridge_car_equipment.car_key` - wyposażenie auta

**Korzyści:**
- Jeden spójny profil samochodu dla wszystkich analiz
- Łatwe połączenie: rentale → serwisy → wyposażenie
- Pre-agregowane metryki (`total_revenue`, `total_service_cost`) dostępne wszędzie

#### 3. dim_store
Używany przez:
- `fact_rental.store_key` - gdzie wypożyczono
- `fact_service.store_key` - gdzie serwisowano
- `dim_car.store_key` - "domowa" lokalizacja auta
- `dim_staff.store_key` - gdzie pracuje pracownik

**Korzyści:**
- Centralna definicja lokalizacji geograficznej
- Spójne analizy geograficzne (miasto, kraj)
- Łatwe agregacje per sklep

### Wymiary niesówdzielone (specyficzne dla jednej tabeli faktów):

- `dim_customer` - tylko `fact_rental` (klienci nie mają związku z serwisami)
- `dim_staff` - tylko `fact_rental` (pracownicy obsługują wypożyczenia, nie serwisy)
- `dim_payment` - tylko `fact_rental` (płatności dotyczą wypożyczeń)
- `dim_equipment` - przez `bridge` tylko związane z `dim_car`

---

## Mapowanie pytań biznesowych

### Raport 1: Analiza klientów (Marketing)

| Pytanie biznesowe | Tabele | Metryka/Atrybut |
|-------------------|--------|-----------------|
| Kim są nasi klienci (wiek)? | `dim_customer` | `YEAR(CURRENT_DATE) - YEAR(birth_date)` |
| Miejsce wypożyczenia | `fact_rental → dim_store` | `GROUP BY city` |
| Skąd przyjeżdżają? | `dim_customer` | `GROUP BY city, country` |
| Jakie marki wypożyczają? | `fact_rental → dim_car` | `GROUP BY producer` |
| Wiek auta | `dim_car` | `YEAR(CURRENT_DATE) - production_year` |
| Długość wypożyczenia | `fact_rental` | `AVG(rental_duration_days)` |
| Czy klienci wracają? | `fact_rental` | `COUNT(*) BY customer_id WHERE count > 2` |
| Jakie wyposażenie wybierają? | `fact_rental → dim_car → bridge → dim_equipment` | `COUNT(*) BY equipment.name` |

**Przykład Power BI DAX:**
```DAX
Customer_Age = YEAR(TODAY()) - YEAR(dim_customer[birth_date])

Returning_Customers =
  CALCULATE(
    DISTINCTCOUNT(fact_rental[customer_key]),
    FILTER(
      VALUES(fact_rental[customer_key]),
      CALCULATE(COUNTROWS(fact_rental)) > 2
    )
  )
```

---

### Raport 2: Przeterminowane płatności (Sprzedaż)

| Pytanie biznesowe | Tabele | Metryka/Atrybut |
|-------------------|--------|-----------------|
| Kim są klienci z opóźnieniami? | `fact_rental → dim_payment (late) → dim_customer` | Demographics WHERE is_late_payment |
| Jakie wypożyczenia mają opóźnienia? | `fact_rental → dim_payment → dim_car` | Rentals WHERE is_late_payment |
| Średnie opóźnienie | `dim_payment` | `AVG(days_overdue) WHERE days_overdue > 0` |
| Gdzie powstają opóźnienia? | `fact_rental → dim_store` | `COUNT(late) BY store_key` |

**Przykład SQL:**
```sql
-- Profil klientów z przeterminowanymi płatnościami:
SELECT
  c.city,
  c.country,
  YEAR(CURRENT_DATE) - YEAR(c.birth_date) as age,
  COUNT(DISTINCT r.rental_key) as late_rentals,
  AVG(p.days_overdue) as avg_days_overdue
FROM fact_rental r
JOIN dim_payment p ON r.payment_key = p.payment_key
JOIN dim_customer c ON r.customer_key = c.customer_key
WHERE p.is_late_payment = TRUE
GROUP BY c.city, c.country, c.birth_date
```

---

### Raport 3: Analiza serwisu

| Pytanie biznesowe | Tabele | Metryka/Atrybut |
|-------------------|--------|-----------------|
| Koszty serwisu (KPI YoY) | `fact_service → dim_date` | `SUM(service_cost) BY year` |
| Ile działań per miesiąc? | `fact_service → dim_date` | `COUNT(*) BY month` |
| Ile działań per samochód? | `fact_service` | `COUNT(*) BY car_key, month` |
| Ranking marek wg kosztów | `fact_service → dim_car` | `SUM(service_cost) BY producer ORDER BY DESC` |

**Przykład Power BI DAX:**
```DAX
Service_Cost_Current_Year =
  CALCULATE(
    SUM(fact_service[service_cost]),
    dim_date[year] = YEAR(TODAY())
  )

Service_Cost_Previous_Year =
  CALCULATE(
    SUM(fact_service[service_cost]),
    dim_date[year] = YEAR(TODAY()) - 1
  )

Service_Cost_YoY_Change =
  [Service_Cost_Current_Year] - [Service_Cost_Previous_Year]

Service_Cost_YoY_Percent =
  DIVIDE(
    [Service_Cost_YoY_Change],
    [Service_Cost_Previous_Year],
    0
  )
```

---

### Raport 4: Analiza operacyjna

| Pytanie biznesowe | Tabele | Metryka/Atrybut |
|-------------------|--------|-----------------|
| Przychód (YoY) | `fact_rental → dim_date` | `SUM(rental_rate) BY year` |
| Podział na sprzedawców | `fact_rental → dim_staff` | `SUM(rental_rate) BY staff_key` |
| Podział na lokalizacje | `fact_rental → dim_store` | `SUM(rental_rate) BY store_key` |
| Zaległości klientów | `fact_rental → dim_payment` | `COUNT(*) WHERE is_late_payment BY store` |
| Ranking wypożyczalni | `fact_rental → dim_store` | `SUM(rental_rate) BY store ORDER BY DESC` |

**Przykład SQL:**
```sql
-- Ranking sprzedawców z hierarchią managera:
SELECT
  s.first_name || ' ' || s.last_name as employee,
  s.manager_name,
  st.city as store_city,
  COUNT(r.rental_key) as rental_count,
  SUM(r.rental_rate) as total_revenue
FROM fact_rental r
JOIN dim_staff s ON r.staff_key = s.staff_key
JOIN dim_store st ON r.store_key = st.store_key
GROUP BY s.staff_key, s.first_name, s.last_name, s.manager_name, st.city
ORDER BY total_revenue DESC
```

---

### Raport 5: Analiza COVID

| Pytanie biznesowe | Tabele | Metryka/Atrybut |
|-------------------|--------|-----------------|
| Zmiana liczby wypożyczeń | `fact_rental → dim_date` | `COUNT(*) BY is_pre_covid/is_covid/is_post_covid` |
| Zmiana struktury samochodów | `fact_rental → dim_car → dim_date` | `COUNT(*) BY producer, covid_period` |
| Zmiana miejsc wypożyczeń | `fact_rental → dim_store → dim_date` | `COUNT(*) BY city, covid_period` |
| Zmiana liczby klientów | `fact_rental → dim_customer → dim_date` | `COUNT DISTINCT(customer_id) BY covid_period` |
| Zmiana profilu klienta | `fact_rental → dim_customer → dim_date` | Demographics BY covid_period |
| Zmiana wyposażenia | `fact_rental → dim_car → bridge → dim_equipment → dim_date` | `COUNT BY equipment, covid_period` |

**Przykład Power BI DAX:**
```DAX
Rentals_Pre_COVID =
  CALCULATE(
    COUNTROWS(fact_rental),
    dim_date[is_pre_covid] = TRUE
  )

Rentals_COVID =
  CALCULATE(
    COUNTROWS(fact_rental),
    dim_date[is_covid] = TRUE
  )

Rentals_Post_COVID =
  CALCULATE(
    COUNTROWS(fact_rental),
    dim_date[is_post_covid] = TRUE
  )

COVID_Impact =
  [Rentals_COVID] - [Rentals_Pre_COVID]

COVID_Recovery =
  [Rentals_Post_COVID] - [Rentals_COVID]
```

**Przykład SQL - porównanie struktur:**
```sql
-- Zmiana popularności marek przed/w trakcie/po COVID:
SELECT
  c.producer,
  SUM(CASE WHEN d.is_pre_covid THEN 1 ELSE 0 END) as pre_covid_rentals,
  SUM(CASE WHEN d.is_covid THEN 1 ELSE 0 END) as covid_rentals,
  SUM(CASE WHEN d.is_post_covid THEN 1 ELSE 0 END) as post_covid_rentals
FROM fact_rental r
JOIN dim_car c ON r.car_key = c.car_key
JOIN dim_date d ON r.rental_date_key = d.date_key
GROUP BY c.producer
ORDER BY covid_rentals DESC
```

---

### Dodatkowe pytania:

| Pytanie biznesowe | Tabele | Metryka/Atrybut |
|-------------------|--------|-----------------|
| Ile zarobiliśmy? | `fact_rental` | `SUM(rental_rate)` |
| Typowe wypożyczenie (długość, koszt) | `fact_rental` | `AVG(rental_duration_days)`, `AVG(rental_rate)` |
| Typowe wypożyczenie (marka) | `fact_rental → dim_car` | `MODE(producer)` lub najpopularniejsza |
| Samochody wolne vs wypożyczone | `dim_car` vs `fact_rental` | LEFT JOIN, NULL = wolne |

**Power BI - wolne samochody:**
```DAX
Total_Cars = COUNTROWS(dim_car)

Currently_Rented_Cars =
  CALCULATE(
    DISTINCTCOUNT(fact_rental[car_key]),
    fact_rental[rental_date] <= TODAY(),
    OR(
      fact_rental[return_date] >= TODAY(),
      ISBLANK(fact_rental[return_date])
    )
  )

Available_Cars = [Total_Cars] - [Currently_Rented_Cars]
```

---

## Proces ETL

### Kolejność ładowania

```
KROK 1: Statyczne wymiary (jednorazowe)
├── dim_date (2018-2030, z flagami COVID)
└── dim_equipment (katalog wyposażenia)

KROK 2: Wymiary podstawowe
├── dim_store
└── dim_customer (z logiką SCD Type 2)

KROK 3: Wymiary zależne
├── dim_staff (zależy od dim_store)
└── dim_car (zależy od dim_store)

KROK 4: Tabele pomocnicze
├── bridge_car_equipment (zależy od dim_car i dim_equipment)
└── dim_payment (niezależny)

KROK 5: Tabele faktów
├── fact_rental (zależy od wszystkich wymiarów)
└── fact_service (zależy od dim_car, dim_date, dim_store)

KROK 6: Aktualizacja agregatów
└── UPDATE dim_car (total_revenue, total_service_cost)
```

---

### SCD Type 2 - dim_customer

**Logika przetwarzania:**

```sql
-- Pseudokod ETL dla SCD Type 2

FOR EACH customer w systemie źródłowym:

  1. Pobierz aktualny rekord z hurtowni:
     SELECT * FROM dim_customer
     WHERE customer_id = source.customer_id
     AND is_current = TRUE

  2. Porównaj city i country:
     IF (hurtownia.city != źródło.city) OR (hurtownia.country != źródło.country):

       a) Zamknij stary rekord:
          UPDATE dim_customer
          SET is_current = FALSE,
              end_date = CURRENT_DATE
          WHERE customer_key = old_record.customer_key

       b) Wstaw nowy rekord:
          INSERT INTO dim_customer (
            customer_id,     -- ten sam
            customer_key,    -- NOWY (auto-increment)
            city,            -- nowy
            country,         -- nowy
            effective_date,  -- CURRENT_DATE
            end_date,        -- NULL
            is_current       -- TRUE
          )

     ELSE:
       -- Brak zmian, nic nie rób
```

**Join w fact_rental (temporal accuracy):**

```sql
-- Podczas ładowania fact_rental, znajdź właściwą wersję klienta:

INSERT INTO fact_rental (customer_key, ...)
SELECT
  c.customer_key,  -- Surrogate key z odpowiedniej wersji
  ...
FROM source_rental r
JOIN dim_customer c
  ON r.customer_id = c.customer_id
  AND r.rental_date BETWEEN c.effective_date
  AND COALESCE(c.end_date, '9999-12-31')
```

**Rezultat:**
`fact_rental.customer_key` wskazuje na wersję klienta, która była aktualna w momencie wypożyczenia.

---

### Pre-agregaty w dim_car

**Aktualizacja total_revenue:**

```sql
UPDATE dim_car c
SET total_revenue = (
  SELECT COALESCE(SUM(rental_rate), 0)
  FROM fact_rental
  WHERE car_key = c.car_key
)
```

**Aktualizacja total_service_cost:**

```sql
UPDATE dim_car c
SET total_service_cost = (
  SELECT COALESCE(SUM(service_cost), 0)
  FROM fact_service
  WHERE car_key = c.car_key
)
```

**Częstotliwość:**
- Dziennie (dla codziennych raportów)
- Lub po każdym ładowaniu fact_rental/fact_service

**Power BI - wybór źródła:**
```DAX
// Opcja 1: Użyj pre-agregatu (szybkie KPI)
Total_Revenue = SUM(dim_car[total_revenue])

// Opcja 2: Policz z faktów (dokładne, drill-down)
Total_Revenue_Detailed = SUM(fact_rental[rental_rate])
```

---

### Flagi COVID w dim_date

**Populacja (jednorazowa):**

```sql
UPDATE dim_date
SET
  is_pre_covid = CASE
    WHEN date < '2020-03-01' THEN TRUE
    ELSE FALSE
  END,
  is_covid = CASE
    WHEN date >= '2020-03-01' AND date <= '2022-06-30' THEN TRUE
    ELSE FALSE
  END,
  is_post_covid = CASE
    WHEN date > '2022-06-30' THEN TRUE
    ELSE FALSE
  END
```

Wykonywane raz podczas inicjalizacji `dim_date`.

---

### Obliczanie dim_payment

**Łączenie z rental dla payment_deadline:**

```sql
INSERT INTO dim_payment (
  payment_id,
  amount,
  payment_date,
  payment_deadline,
  is_late_payment,
  days_overdue
)
SELECT
  p.payment_id,
  p.amount,
  p.payment_date,
  r.payment_deadline,
  CASE
    WHEN p.payment_date > r.payment_deadline THEN TRUE
    ELSE FALSE
  END as is_late_payment,
  DATEDIFF(day, r.payment_deadline, p.payment_date) as days_overdue
FROM source_payment p
JOIN source_rental r ON p.rental_id = r.rental_id
```

**Metryki pre-kalkulowane** dla wydajności - nie trzeba liczyć w każdym zapytaniu.

---

### Harmonogram ETL (przykładowy)

```
INICJALIZACJA (raz):
├── dim_date (load 2018-2030)
├── dim_equipment (load catalog)
└── dim_store (load initial stores)

DAILY ETL (codziennie 02:00):
├── 1. Extract from source (wheelie DB)
├── 2. Load/Update dim_customer (SCD Type 2 check)
├── 3. Load/Update dim_staff
├── 4. Load/Update dim_car
├── 5. Load/Update bridge_car_equipment
├── 6. Load dim_payment (new payments)
├── 7. Load fact_rental (previous day)
├── 8. Load fact_service (previous day)
├── 9. Update dim_car aggregates
└── 10. Data quality checks

WEEKLY ETL (niedziela 04:00):
├── Full recalculation of dim_car aggregates
├── Validate SCD Type 2 integrity
└── Generate ETL summary reports
```

---

## Podsumowanie

### Mocne strony architektury:

✅ **Skalowalność:** Obsługuje miliony wypożyczeń i serwisów
✅ **Wydajność:** Pre-agregaty w dim_car, pre-kalkulowane flagi
✅ **Elastyczność:** Łatwo dodać nowe wymiary lub fakty
✅ **Czytelność:** Jasna struktura star schema
✅ **Reużywalność:** Współdzielone wymiary (dim_date, dim_store, dim_car)
✅ **Akademickość:** Demonstracja SCD Type 2 i bridge table

### Obszary do rozszerzenia w przyszłości:

🔮 **fact_inventory_snapshot:** Dzienne snapshoty stanu aut (wolne/wypożyczone/serwis)
🔮 **dim_customer_segment:** Pre-kalkulowane segmenty klientów (VIP, frequent, occasional)
🔮 **fact_payment_installments:** Jeśli rozszerzyć o płatności ratalne
🔮 **dim_promotion:** Jeśli dodać kody promocyjne i rabaty
🔮 **Partycjonowanie:** dim_date.year dla bardzo dużych danych

---

## Kontakt i wsparcie

Dla pytań technicznych lub biznesowych dotyczących tej hurtowni danych, skontaktuj się z zespołem Data Engineering.

**Wersja dokumentacji:** 1.0
**Data utworzenia:** 10 grudnia 2025
**Ostatnia aktualizacja:** 10 grudnia 2025
