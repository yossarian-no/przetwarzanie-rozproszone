# przetwarzanie-rozproszone

## Grupa projektowa i temat projektu

Vasil Kusmartsev 156202 
Mateusz Kaźmierczak 160162 
Przedmiot: Przetwarzanie Rozproszone, laboratoria, 6 semestr
Temat: Stowarzyszenie Umarłych Poetów

Projekt implementuje rozproszony algorytm wzajemnego wykluczania dla grup procesów, oparty na algorytmie Ricarta-Agrawali. Program symuluje grupę poetów, którzy cyklicznie zbierają się w kółka poetyckie w celu wspólnej libacji.

## Stowarzyszenie umarłych poetów

W pewnym mieście uniwersyteckim grupa młodych ludzi wpadła na pomysł, by zwiększyć swoje zdolności
twórcze konsumpcją dużej ilości alkoholu. W tym celu tworzą kółka poetyckie, po czym każdy przynosi
alkohol lub zagrychę, przeprowadzają libację, przy okazji usiłując spłodzić dzieła mające przebić
Przybyszewskiego. Po libacji poeta jakiś czas odpoczywa. Poeci działają pod natchnieniem chwili, więc
mogą zdecydować w sposób losowy, że do jakiegoś kółka nie chcą przynależeć.

Danych jest P poetów. Poeci dobierają się w kółka o wielkości K. Po zebraniu w kółko poeci decydują, kto przyniesie
alkohol, a kto zagrychę, a kto może pić na sępa. Poeci nie lubią być frajerami, więc nie mogą non stop
przynosić jednego rodzaju towaru lub sępić. Po libacji poeci odpoczywają (i mają różną odporność na alkohol, więc
każdy może odpoczywać inny czas).

## Działanie

Każdy proces (poeta) działa w nieskończonej pętli:

- **Odpoczywa** przez losowy czas
- **Ubiega się o wejście do kółka** — rozsyła żądania `REQUEST` do wszystkich pozostałych procesów
- **Uczestniczy w libacji** w grupie K poetów
- **Czeka na zakończenie** całego kółka przed zwolnieniem miejsca w kolejce

Wewnątrz każdego kółka poeci przydzielają sobie role w sposób sprawiedliwy:

- 🍶 **Przynoszący alkohol** — ten, kto najrzadziej pełnił tę rolę
- 🥗 **Przynoszący zagrychę** — analogicznie
- 🦅 **Sęp** — pozostali, którzy tylko piją

## Stany procesu

Każdy proces przechodzi przez cztery stany:

| Stan | Opis |
|------|------|
| `RESTING` | Poeta odpoczywa po libacji |
| `SEEKING` | Poeta ubiega się o wejście do kółka |
| `IN_CIRCLE` | Poeta uczestniczy w libacji |
| `TOMBSTONE` | Poeta skończył libację, czeka na resztę kółka |

## Typy wiadomości MPI

| Wiadomość | Opis |
|-----------|------|
| `REQUEST` | Zgłoszenie chęci wejścia do kółka |
| `ACK` | Potwierdzenie odbioru żądania |
| `ROLE_INFO` | Wymiana historii ról między partnerami w kółku |
| `RELEASE` | Zwolnienie miejsca w kolejce po zakończeniu libacji |
| `DONE` | Informacja o zakończeniu libacji przez jednego poetę |

## Technologie

- **MPI** (OpenMPI) — komunikacja między procesami
- **POSIX Threads** — każdy proces używa dwóch wątków: głównego i odbiorczego
- **Zegary Lamporta** — logiczne uporządkowanie zdarzeń w systemie rozproszonym
- **Język C**

## Wymagania

- System Linux (np. Ubuntu)
- OpenMPI

```bash
sudo apt update
sudo apt install build-essential openmpi-bin openmpi-doc libopenmpi-dev
```

## Kompilacja i uruchomienie

```bash
# Kompilacja
mpicc -o poeci main.c -lpthread

# Uruchomienie (P procesów, kółka po K osób)
mpiexec -n 4 ./poeci 3
```

## Przykładowy output

```
[0] [t0] Spie
[1] [t0] Spie
[2] [t1] Rozpoczynam staranie o kolko
[1] [t6] Rozpoczynam staranie o kolko
[0] [t22] Jestem w kolku [0,1,2] jako przynoszacy alkohol
[1] [t23] Jestem w kolku [0,1,2] jako przynoszacy zakaske
[2] [t22] Jestem w kolku [0,1,2] jako sep (tylko pije)
[0] [t28] Skonczylem biesiade, czekam na reszte kolka (nagrobek)
[0] [t34] Cale kolko skonczylo, zwalniam miejsce
```

Format każdej linii: `[rank] [tCzasLogiczny] wiadomość`

