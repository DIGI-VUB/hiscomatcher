# HISCO matcher

- App die beroepen matcht met HISCO beroepen
- Run the app

```
library(shiny)
runGitHub("hiscomatcher", "digi-vub", subdir = "src")
```

- Installation: make sure the following packages are installed 

```
install.packages("shiny")
install.packages("bs4Dash")
install.packages("shinydisconnect")
install.packages("shinyglide")
install.packages("shinyFiles")
install.packages("shinyWidgets")
install.packages("readxl")
install.packages("udpipe")
install.packages("data.table")
install.packages("DT")
install.packages("RSQLite")
install.packages("reactable")
```

- HISCO data beschikbaar in `data/hisco.RData` beschikbaar op https://datasets.iisg.amsterdam/dataset.xhtml?persistentId=hdl:10622/MUZMAL

```
@data{MUZMAL_2018,
author = {Mandemakers, Kees and Mourits, Rick and Muurling, Sanne},
publisher = {IISH Data Collection},
title = {{HSN_HISCO_release_2018_01}},
UNF = {UNF:6:wfK8iY9uetyciZC6YD66Pw==},
year = {2018},
version = {V3},
doi = {10622/MUZMAL},
url = {https://hdl.handle.net/10622/MUZMAL}
}
```