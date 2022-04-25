library(shiny)
library(bs4Dash)
library(shinydisconnect)
library(shinyglide)
library(shinyFiles)
library(shinyWidgets)
library(readxl)
library(writexl)
library(tools)
library(udpipe)
library(data.table)
library(DT)
library(RSQLite)
library(reactable)
library(stringdist)


engine_db <- function(..., DB = dashdata$DB, FUN = dbListTables){
  con <- dbConnect(RSQLite::SQLite(), DB)
  on.exit({
    dbDisconnect(con)  
  })
  FUN(con, ...)
}
write_db <- function(...){
  engine_db(..., FUN = dbWriteTable)
}
read_db <- function(...){
  engine_db(..., FUN = dbGetQuery)
}
table_exists <- function(x){
  x %in% engine_db(FUN = dbListTables)
}
read_matched <- function(){
  if(table_exists("hisco_matched")){
    read_db("select * from hisco_matched")
  }else{
    data.frame(activiteit = character())
  }
}
read_hisco <- function(){
  x <- read_db("select * from hisco_gold")
  #x$activiteit <- strsplit(x$activiteit, split = "\\|")
  #x$activiteit_standard <- strsplit(x$activiteit_standard, split = "\\|")
  x$activiteit_cleaned <- strsplit(x$activiteit_cleaned, split = "\\|")
  x
}
txt_standardiser <- function(x, from = ""){
  ## Put all in ASCII, lowercase, keep only letters and spaces, no special spaces/punctuation symbols
  x <- iconv(x, from = from, to = "ASCII//TRANSLIT")
  x <- tolower(x)
  x <- gsub("[[:space:]+]", " ", x)
  x <- gsub("[^A-Za-z ]", "", x)
  x <- trimws(x)
  x
}

dashdata <- list()
dashdata$DB_home <- R_user_dir(package = "hisco", which = "data")
dashdata$DB      <- file.path(dashdata$DB_home, "hisco.sqlite")
dir.create(dashdata$DB_home, recursive = TRUE, showWarnings = FALSE)
dashdata$url_hisco      <- "https://iisg.amsterdam/en/data/data-websites/history-of-work"
dashdata$url_hisco_data <- "https://datasets.iisg.amsterdam/dataset.xhtml?persistentId=hdl:10622/MUZMAL"
dashdata$HISCO_fields   <- c("Original", "Standard", "HISCO", "STATUS", "RELATION", 
                             "PRODUCT", "HISCLASS", "HISCLASS_5", "HISCAM_U1", "HISCAM_NL", 
                             "SOCPO", "OCC1950", "Release")
load(url("https://github.com/DIGI-VUB/hiscomatcher/raw/master/data/hisco.RData"))
dashdata$HISCO <- hisco
dashdata$HISCO$activiteit_standard  <- dashdata$HISCO$Standard
dashdata$HISCO$activiteit           <- dashdata$HISCO$Original
dashdata$HISCO$activiteit_standard_cleaned  <- txt_standardiser(dashdata$HISCO$activiteit_standard, from = "Latin1")
dashdata$HISCO$activiteit_cleaned           <- txt_standardiser(dashdata$HISCO$activiteit, from = "Latin1")
dashdata$HISCO$ID_GOLD <- as.integer(factor(dashdata$HISCO$activiteit_standard_cleaned))
dashdata$HISCO <- subset(dashdata$HISCO, !is.na(ID_GOLD))
dashdata$GOLD <- as.data.table(dashdata$HISCO)
dashdata$GOLD <- dashdata$GOLD[, list(activiteit = paste(unique(activiteit), collapse = "|"),
                                      activiteit_cleaned = paste(unique(activiteit_cleaned), collapse = "|"),
                                      activiteit_standard = paste(unique(activiteit_standard), collapse = "|"),
                                      Original = paste(unique(Original), collapse = "|"),
                                      Standard = paste(unique(Standard), collapse = "|")), 
                               by = c("ID_GOLD", "activiteit_standard_cleaned", "HISCO", "STATUS", "RELATION", 
                                          "PRODUCT", "HISCLASS", "HISCLASS_5", "HISCAM_U1", "HISCAM_NL", 
                                          "SOCPO", "OCC1950", "Release")]
dashdata$GOLD <- setDF(dashdata$GOLD)
#dashdata$GOLD <- paste.data.frame(data = dashdata$HISCO, 
#                                  term = c("activiteit", "activiteit_cleaned", "activiteit_standard", "Original", "Standard"), 
#                                  group = c("ID_GOLD", "activiteit_standard_cleaned", "HISCO", "STATUS", "RELATION", 
#                                            "PRODUCT", "HISCLASS", "HISCLASS_5", "HISCAM_U1", "HISCAM_NL", 
#                                            "SOCPO", "OCC1950", "Release"), 
#                                  collapse = "|")
# some (6) seem to have a standard term which have differernt HISCO code -> keep only 1 giving preference to higher status and higher HISCO
#View(subset(dashdata$GOLD, ID_GOLD %in% dashdata$GOLD$ID_GOLD[which(duplicated(dashdata$GOLD$ID_GOLD))]))
dashdata$GOLD <- dashdata$GOLD[order(dashdata$GOLD$STATUS, dashdata$GOLD$HISCO, decreasing = TRUE), ]
dashdata$GOLD <- dashdata$GOLD[!duplicated(dashdata$GOLD$ID_GOLD), ]

dashdata$uploaded <- "default"

## Save initial values to the database
write_db(name = "hisco", value = dashdata$HISCO, overwrite = TRUE)
write_db(name = "hisco_gold", value = dashdata$GOLD, overwrite = TRUE)
if(!table_exists("hisco_matched")){
  write_db(name = "hisco_matched", value = data.frame(file = character(), 
                                                      now = as.character(as.POSIXct(character())), 
                                                      .rowid = integer(), 
                                                      .TEXT = character(), 
                                                      ID_GOLD = integer(), 
                                                      GOLD = character(), 
                                                      skipped = logical(),
                                                      stringsAsFactors = FALSE), overwrite = FALSE)
}

# x <- read_hisco()
# x <- as.data.table(x[, c("ID_GOLD", "activiteit_cleaned")])
# x <- x[, lapply(.SD, unlist), by = list(ID_GOLD), .SDcols = "activiteit_cleaned"]
# x <- subset(x)

match_exact <- function(x, fields, GOLD, VARIANTS = dashdata$HISCO){
  ## BASIS term: activiteit_standard_cleaned
  #GOLD$ID_GOLD
  #GOLD$activiteit_standard_cleaned
  #GOLD$activiteit_cleaned
  GOLD_act <- as.data.table(GOLD[, c("ID_GOLD", "activiteit_cleaned")])
  GOLD_act <- GOLD_act[, lapply(.SD, unlist), by = list(ID_GOLD), .SDcols = "activiteit_cleaned"]
  #activiteit_standard_cleaned: naam van gestandaardiseerde activiteit
  #activiteit_cleaned: list of namen van activiteit varianten
  x$.match <- rep(NA_integer_, nrow(x))
  for(field in fields){
    field_first <- x[[field]]
    field_first <- strsplit(field_first, split = ";")
    field_first <- sapply(field_first, FUN = function(x) head(x, n = 1))
    ## exact match on standard 
    x$.match_standard <- txt_recode(field_first, from = GOLD$activiteit_standard_cleaned, to = GOLD$ID_GOLD, na.rm = TRUE)
    ## exact match on all variants
    x$.match_variant  <- txt_recode(field_first, from = GOLD_act$activiteit_cleaned, to = GOLD_act$ID_GOLD, na.rm = TRUE)
    x$.match          <- ifelse(is.na(x$.match), ifelse(is.na(x$.match_standard), x$.match_variant, x$.match_standard), x$.match)
  }
  x$.MATCH <- txt_recode(x$.match, from = VARIANTS$ID_GOLD, to = VARIANTS$Standard, na.rm = TRUE)
  x <- merge(x, GOLD, by.x = ".match", by.y = "ID_GOLD", sort = FALSE, all.x = TRUE)
  x <- x[order(x$.rowid, decreasing = FALSE), ]
  x
}
match_stringdist <- function(x, fields, GOLD, VARIANTS = dashdata$HISCO, top_n = 10){
  closest <- function(x, top_n = 10){
    x <- x[order(x$afstand, decreasing = FALSE), ]
    x <- head(x, n = top_n)
    x
  }
  GOLD_act <- as.data.table(GOLD[, c("ID_GOLD", "activiteit_cleaned")])
  GOLD_act <- GOLD_act[, lapply(.SD, unlist), by = list(ID_GOLD), .SDcols = "activiteit_cleaned"]
  GOLD_act <- subset(GOLD_act, !activiteit_cleaned %in% GOLD$activiteit_standard_cleaned)
  #activiteit_standard_cleaned: naam van gestandaardiseerde activiteit
  #activiteit_cleaned: list of namen van activiteit varianten
  matches <- list()
  for(idx in seq_len(nrow(x))){
    if((idx %% 50) == 0) cat(sprintf("%s %s/%s", Sys.time(), idx, nrow(x)), sep = "\n")
    .rowid <- x$.rowid[idx]
    el <- list()
    for(field in fields){
      host  <- x[[field]][idx]
      host  <- strsplit(host, split = ";")
      host  <- head(unlist(host, use.names = FALSE), n = 1)
      woord <- txt_standardiser(host)
      
      afstanden <- list()
      ## stringdist on standard 
      afstanden$standardised <- stringdist(woord, GOLD$activiteit_standard_cleaned, method = "osa")
      afstanden$standardised <- data.frame(.rowid = .rowid, 
                                           #host = host, 
                                           ID_GOLD = GOLD$ID_GOLD,
                                           GOLD = GOLD$activiteit_standard_cleaned,
                                           ON = field,
                                           afstand = afstanden$standardised, 
                                           stringsAsFactors = FALSE)
      ## stringdist on all variants
      afstanden$variants <- stringdist(woord, GOLD_act$activiteit_cleaned, method = "osa")
      afstanden$variants <- data.frame(.rowid = .rowid, 
                                       #host = host, 
                                       ID_GOLD = GOLD_act$ID_GOLD,
                                       GOLD = GOLD_act$activiteit_cleaned,
                                       ON = field,
                                       afstand = afstanden$variants, 
                                       stringsAsFactors = FALSE)
      afstanden <- lapply(afstanden, FUN = function(x){
        closest(x, top_n = top_n) 
      })
      afstanden <- rbindlist(afstanden)
      afstanden <- closest(afstanden, top_n = top_n)
      el[[field]] <- afstanden
    }
    el <- rbindlist(el)
    el <- el[order(el$afstand, el$GOLD, decreasing = FALSE), ]
    el <- el[!duplicated(el$GOLD), ]
    el <- closest(el, top_n = top_n)
    matches[[idx]] <- el
  }
  matches <- rbindlist(matches)
  x <- merge(x, matches, by.x = ".rowid", by.y = ".rowid", sort = FALSE, all.x = TRUE)
  x <- x[order(x$.rowid, x$afstand, decreasing = FALSE), ]
  x
}

# d <- readxl::read_excel(path = "C:/Users/Jan/Desktop/1 Basislijst beroepen Nederlands.xlsx", sheet = 1, na = "", trim_ws = TRUE, guess_max = floor(.Machine$integer.max/100))
# d$.rowid <- seq_len(nrow(d))
# 
# z <- match_exact(d, GOLD = dashdata$HISCO, fields = c("Kopie van bronbestand", "Gestandaardiseerde beroepstitel"))
# z <- subset(d, d$.rowid %in% z$.rowid[is.na(z$.MATCH)])
# z <- head(z, 100)
# z <- match_stringdist(x = z, fields = c("Kopie van bronbestand", "Gestandaardiseerde beroepstitel"), GOLD = dashdata$GOLD, VARIANTS = dashdata$HISCO)



shinyApp(
  ui = dashboardPage(
    title = "HISCO MATCHER",
    dark = FALSE,
    header = dashboardHeader(
      title = dashboardBrand(
        title = "HOST Beroepen matcher",
        color = "primary",
        href = "https://digi.research.vub.be/",
        #image = "https://adminlte.io/themes/v3/dist/img/AdminLTELogo.png",
        opacity = 0.8
      ),
      fixed = TRUE
    ),
    sidebar = dashboardSidebar(
      fixed = TRUE,
      skin = "light",
      status = "primary",
      id = "sidebar",
      sidebarUserPanel(
        image = "https://avatars.githubusercontent.com/u/62653831?s=200&v=4",
        name = tags$a("by DIGI", href = "https://digi.research.vub.be/home")
      ),
      disconnectMessage(
        text = "You are not using the app and are now disconnected.",
        refresh = "Restart the app",
        width = 450,
        top = 50,
        size = 22,
        background = "white",
        colour = "#444444",
        overlayColour = "black",
        overlayOpacity = 0.6,
        refreshColour = "#337ab7",
        css = ""
      ),
      sidebarMenu(
        id = "current_tab",
        flat = FALSE,
        compact = FALSE,
        childIndent = TRUE,
        sidebarHeader("Match beroepen met HISCO"),
        menuItem(
          "Laadt je data op",
          badgeLabel = "Start",
          badgeColor = "success",
          tabName = "tab_upload",
          icon = icon("upload")
        ),
        menuItem(
          "Match",
          tabName = "tab_start_matching",
          icon = icon("laptop-code")
        ),
        sidebarHeader("Resultaat"),
        menuItem(
          text = "Data Export",
          icon = icon("cubes")
        ),
        downloadButton(outputId = "uo_download_results", label = "Export")
      )
    ),
    body = dashboardBody(
      tags$head(
        tags$style(css <- "
        .container-fluid {
          padding: 0 30px;
        }
        .shinyglide {
          border: 1px solid #888;
          box-shadow: 0px 0px 10px #888;
          padding: 1em;
        }")
      ),
      tabItems(
        tabItem(tabName = "tab_start_matching", 
                fluidPage(title = "MATCHING",
                          fluidRow(
                            column(
                              width = 10,
                              tabBox(
                                ribbon(
                                  text = "Matching",
                                  color = "pink"
                                ),
                                elevation = 2,
                                id = "tabmatching",
                                width = 12,
                                collapsible = FALSE, 
                                closable = FALSE,
                                type = "tabs",
                                status = "primary",
                                solidHeader = TRUE,
                                selected = "Exact gematcht",
                                tabPanel(title = "Exact gematcht",
                                         tags$blockquote("Deze dataset toont termen die exact konden gematcht worden (ofwel adhv de standaard of variant)"),
                                         checkboxGroupButtons(inputId = "ui_groupby", label = "Groepeer volgens", status = "primary", 
                                                              choices = c("Geen", setdiff(dashdata$HISCO_fields, c("Original", "Standard"))), selected = NULL),
                                         reactableOutput(outputId = "uo_exact")
                                ),
                                tabPanel(title = "Te Valideren",
                                         tags$blockquote("Deze dataset toont termen die niet 100% exact konden gematcht worden en dus validatie vereisen. We tonen HISCO beroepen die gelijkaardig zijn via de Levehnstein afstand met jouw beroepen. Selecteer links welke correct is. Die selectie wordt dan bewaard en de app laadt dan de volgende te valideren match."),
                                         #actionButton(inputId = "ui_zoek", label = "Zoek", icon = icon("search")),
                                         actionButton(inputId = "ui_skip", label = "Sla over", icon = icon("fast-forward")),
                                         actionButton(inputId = "ui_toon_werk", label = "Toon reeds gevalideerde"),
                                         searchInput(
                                           inputId = "ui_zoek", 
                                           label = "Zoek zelf in HISCO:", 
                                           placeholder = "zet je zoekterm hier", 
                                           btnSearch = icon("search"), 
                                           btnReset = icon("remove"), 
                                           width = "30%"
                                         ),
                                         reactableOutput(outputId = "uo_valideer")
                                )
                              )
                            ),
                            column(width = 2,
                                   uiOutput(outputId = "uo_stats_records_own"),
                                   uiOutput(outputId = "uo_stats_records_hisco"),
                                   box(
                                     solidHeader = FALSE,
                                     title = "Match progress",
                                     background = NULL,
                                     width = 12,
                                     status = "info",
                                     footer = fluidRow(
                                       column(
                                         width = 12,
                                         uiOutput(outputId = "uo_stats_matching_percent")
                                       ),
                                       column(
                                         width = 12,
                                          uiOutput(outputId = "uo_stats_matching_manual")
                                       )
                                     )
                                   ))  
                          )
                          )),
        tabItem(tabName = "tab_upload", 
                fluidRow(
                  column(width = 2),
                  column(width = 8, 
                         glide(
                           height = "100%",
                           screen(
                             tags$h4("Data opladen"),
                             p("U kunt hier je data opladen. Dit kan in CSV of EXCEL formaat.",
                               tags$ul(
                                 tags$li("De eerste kolom dient een ID te zijn die je wil gebruiken om later terug te linken"),
                                 tags$li("de 2e kolom bevat je beroepen die we zullen gebruiken om te matchen aan HISCO")
                               )),
                             shinyFilesButton('ui_upload_file', label = 'Selecteer je bestand', title = 'Selecteer de EXCEL/CSV file', 
                                              multiple = FALSE, icon = icon("file-excel")),
                             uiOutput('ui_uploaded_file'),
                             next_label = "Volgende stap", previous_label = "Vorige stap"#, next_condition = "!is.null(input.ui_upload_file)"
                           ),
                           screen(
                             tags$h4("Selecteer tekst veld(en) met beroepen om te matchen met HISCO"),
                             uiOutput(outputId = "uo_selected_fields"),
                             selectInput("ui_fields", label = "Selecteer andere velden uit je dataset", choices = NULL, multiple = TRUE),
                             dataTableOutput(outputId = "uo_rawdata"),
                             next_label = "Volgende stap", previous_label = "Vorige stap"
                           ),
                           screen(
                             p(tags$h4("Deze tool matcht de beroepen uit je eigen dataset met de HISCO data")),
                             p("Dit gebeurt door"),
                             tags$ul(
                               tags$li("Te kijken naar termen uit de HISCO database die gelijkaardig geschreven zijn als jouw beroepen"),
                               tags$li("Te kijken naar termen uit de HISCO database die semantisch gelijkaardig zijn als jouw beroepen")
                             ),
                             tags$br(),
                             actionButton(inputId = "ui_start_matching", label = "START", status = "success", size = "lg", icon = icon("play")),
                             previous_label = "Vorige stap"
                           )
                         )),
                  column(width = 2)  
                ))
      )
    ),
    controlbar = dashboardControlbar(
      id = "controlbar",
      skin = "light",
      pinned = TRUE,
      overlay = FALSE,
      controlbarMenu(
        id = "controlbarMenu",
        type = "pills",
        controlbarItem(
          "Algemene settings",
          column(
            width = 12,
            align = "center",
            sliderInput(inputId = "ui_matching_stringdist_topn", min = 1, max = 5000, value = 250, label = "Toon top-n matches")
          )
        )
      )
    ),
    footer = dashboardFooter(
      fixed = FALSE,
      left = a(
        href = "https://digi.research.vub.be/",
        target = "_blank", "DIGI VUB"
      ),
      right = paste("2022", "using database at", dashdata$DB)
    )
  ),
  server = function(input, output, session) {
    showModal(modalDialog(
      title = "Welkom",
      tags$p("Het objectief van deze app is het matchen van beroepen aan de ", tags$a("HISCO", href = dashdata$url_hisco), " dataset om een gestandaardiseerde set te bekomen van beroepen. 
  Hoe doe je dit?"),
      tags$ul(
        tags$li("Laadt je data met beroepen op in EXCEL/CSV formaat"),
        tags$li("Er gebeurt een automatische match"),
        tags$li("Je valideert de matches")
      ),
      "De gestandaardiseerde set wordt gesaved en is te vinden op volgende online link.",
      easyClose = TRUE,
      footer = NULL
    ))
    dirs <- c(HOME = Sys.getenv("HOME"), 
              USER = Sys.getenv("R_USER"), 
              HOMEDRIVE = Sys.getenv("HOMEDRIVE"), 
              CURRENT_DIR = getwd(),
              DESKTOP = file.path(Sys.getenv("USERPROFILE"), "Desktop"))
    dirs <- na.exclude(dirs)
    ##################################################################################v
    ## UI of data uploading
    ##
    shinyFileChoose(input, id = 'ui_upload_file', roots = dirs, filetypes = c('csv', 'xlsx', 'xls'))
    uploaded_file <- reactive({
      path <- parseFilePaths(dirs, input$ui_upload_file)
      if(nrow(path) > 0){
        dashdata$uploaded <<- basename(path$name)
      }
      path <- path$datapath
      path
    })
    uploaded_file_read <- reactive({
      p <- uploaded_file()
      if(length(p) > 0 && file.exists(p)){
        
        ext <- file_ext(p)
        if(ext == "xlsx"){
          d <- readxl::read_excel(path = p, sheet = 1, na = "", trim_ws = TRUE, guess_max = floor(.Machine$integer.max/100))
        }else{
          d <- readLines(p, n = 1)
          if(txt_count(d, pattern = ",") > txt_count(d, pattern = ";")){
            d <- read.csv(p, header = TRUE, sep = ",", na.strings = "", row.names = FALSE)
          }else{
            d <- read.csv2(p, header = TRUE, sep = ";", na.strings = "", row.names = FALSE)
          }
        }
      }else{
        d <- data.frame()
      }
      out <- list(data = d, fields = colnames(d), field_content = tail(head(colnames(d), n = 2), n = 1))
      out$data$.rowid <- seq_len(nrow(out$data))
      out
    })
    output$uo_rawdata <- renderDataTable({
      ds <- uploaded_file_read()
      datatable(ds$data, rownames = FALSE, options = list(
        columnDefs = list(list(className = 'dt-center', targets = 5)),
        pageLength = 3,
        lengthMenu = c(3, 5, 10, 15, 20)
      ))
    })
    observeEvent(input$ui_start_matching, {
      updatebs4TabItems(session = session, inputId = "current_tab", selected = "tab_start_matching")
    })
    observe({
      ds <- uploaded_file_read()
      updateSelectInput(session, inputId = "ui_fields", choices = ds$fields, selected = ds$field_content)
    })
    output$uo_selected_fields <- renderUI({
      ds <- uploaded_file_read()
      if(length(input$ui_fields) > 0){
        texts <- ds$data[, input$ui_fields]
        texts <- unlist(texts, use.names = FALSE)
        out <- tags$p(
          icon("columns"), "Veld(en) te gebruiken om te matchen: ", paste(input$ui_fields, collapse = " & "),
          tags$ul(
            tags$blockquote("Voorbeeld:", txt_collapse(txt_sample(texts, na.exclude = TRUE, n = 3), collapse = ", "))
          )
        )
      }else{
        out <- tags$p(
          tags$blockquote(icon("columns"), "Selecteer een aantal velden om te matchen met HISCO")
        )
      }
    })
    output$ui_uploaded_file <- renderUI({
      x <- basename(uploaded_file())
      if(length(x)){
        out <- tags$p(
          tags$br(),
          tags$br(),
          tags$h4("Geselecteerde file: "),
          tags$ul(tags$li(
            icon("file"),
            x  
          )) 
        )
        return(out)
      }else{
        NULL
      }
    })
    ##################################################################################v
    ## UI of matching
    ##
    uploaded_file_matchinfo <- reactive({
      userdata <- uploaded_file_read()
      match_on <- input$ui_fields
      top_n    <- input$ui_matching_stringdist_topn 
      list(userdata = userdata, match_on = match_on, top_n = top_n)
    })
    DB_HISCO <- reactive({
      input$ui_upload_file
      #ds <- dashdata$HISCO
      ds <- read_hisco()
      ds
    })
    DB_matched <- reactive({
      x <- read_matched()
      x
    })
    output$uo_stats_records_own <- renderUI({
      ds <- uploaded_file_read()
      n  <- nrow(ds$data)
      infoBox(title = "Aantal records", subtitle = "in jouw dataset", value = n, color = "success", icon = icon("database"), width = 12, elevation = 4)
    })
    output$uo_stats_records_hisco <- renderUI({
      ds <- DB_HISCO()
      n  <- nrow(ds)
      infoBox(title = "Aantal records", subtitle = "in HISCO", value = n, color = "info", icon = icon("sliders-h"), width = 12, elevation = 4)
    })
    matching_data <- reactive({
      userdata <- uploaded_file_read()
      hisco    <- DB_HISCO()
      matched  <- DB_matched()
      list(hisco = hisco, userdata = userdata, matched = matched)
    })
    matcher_exact <- reactive({
      matchinfo <- uploaded_file_matchinfo()
      hisco     <- DB_HISCO()
      if(nrow(matchinfo$userdata$data) > 0){
        showModal(modalDialog("Zoekt naar zo goed als exacte matches tussen HISCO en jouw dataset. Van zodra je data ziet verschijnen kan je beginnen werken.", footer = NULL, easyClose = TRUE))
        on.exit({removeModal()})  
      }
      x         <- match_exact(matchinfo$userdata$data, GOLD = hisco, fields = matchinfo$match_on)
      list(rawdata = matchinfo$userdata$data, exact = x, hisco = hisco, matchinfo = matchinfo)
    })
    output$uo_exact <- renderReactable({
      ds <- matcher_exact()
      matchinfo <- ds$matchinfo
      show_hisco_fields <- setdiff(dashdata$HISCO_fields, c("Original", "Standard"))
      x <- subset(ds$exact, !is.na(HISCO), select = c(".MATCH", matchinfo$userdata$fields, show_hisco_fields))
      groupby <- NULL
      if(length(input$ui_groupby) > 0){
        groupby   <- input$ui_groupby    
        if("Geen" %in% groupby){
          groupby   <- NULL
        }
      }
      if(nrow(x) > 0){
        lst <- list(.MATCH = colDef(name = "HISCO", minWidth = 200))
        for(i in matchinfo$match_on){
          lst[[i]] <- colDef(minWidth = 200)
        }
        reactable(x, 
                  sortable = TRUE, filterable = TRUE, searchable = TRUE, resizable = TRUE, 
                  showPageSizeOptions = TRUE, pageSizeOptions = c(3, 5, 10, 15, 20, 50, 100, 1000), defaultPageSize = 5,
                  borderless = TRUE,
                  groupBy = groupby, defaultColDef = colDef(aggregate = "unique"),
                  columns = lst,
                  columnGroups = list(
                    colGroup(name = "MATCH", columns = ".MATCH", align = "left", headerStyle = list(fontWeight = 700)),
                    colGroup(name = "Uw data", columns = matchinfo$userdata$fields, align = "left", headerStyle = list(fontWeight = 700)),
                    colGroup(name = "HISCO data", columns = show_hisco_fields, align = "left", headerStyle = list(fontWeight = 700))
                  )) 
      }
    })
    done_manually <- reactiveVal(value = 0, label = "Aantal achter de rug")
    get_next <- reactive({
      done_manually()
      DB        <- matcher_exact()
      top_n     <- 1000
      top_n     <- DB$matchinfo$top_n
      d         <- DB$rawdata
      # exclude exact matches
      d         <- subset(d, d$.rowid %in% DB$exact$.rowid[is.na(DB$exact$.MATCH)])
      if(nrow(d) == 0){
        return(data.frame())
      }
      # exclude previously done inexact matches
      d$.TEXT      <- apply(d[,  DB$matchinfo$match_on, drop = FALSE], MARGIN = 1, FUN = txt_collapse, collapse = "::::")
      already_done <- read_db('select ".rowid", ".TEXT" from hisco_matched')
      d            <- subset(d, !d$.TEXT %in% already_done$.TEXT)
      d            <- subset(d, !is.na(d$.TEXT))
      #x <- d[sample.int(n = nrow(d), size = 1), ]
      x <- head(d, n = 1)
      if(nrow(d) == 0){
        return(data.frame())
      }
      showModal(modalDialog("Zoekt naar beroepen die lijken op:", x$.TEXT, footer = NULL))
      updateSearchInput(session, inputId = "ui_zoek", value = "", trigger = FALSE)
      x <- match_stringdist(x, fields = DB$matchinfo$match_on, DB$hisco, top_n = +Inf)
      removeModal()
      #x <- iris[sample.int(n = nrow(iris), size = nrow(iris)), ]
      keep <- c(".rowid", DB$matchinfo$match_on, "GOLD", "afstand", "ID_GOLD")
      x$thisone <- rep(NA, nrow(x))
      x <- subset(x, select = c("thisone", keep, ".TEXT"))
      x$.row <- seq_len(nrow(x))
      x
    })
    output$uo_valideer <- renderReactable({
      x <- get_next()
      x <- head(x, n = input$ui_matching_stringdist_topn)
      reactable(x,
                columns = list(
                  .row = colDef(show = FALSE),
                  .rowid = colDef(show = FALSE),
                  .TEXT = colDef(show = FALSE),
                  ID_GOLD = colDef(show = FALSE, minWidth = 50, name = "ID_GOLD", align = "right"),
                  afstand = colDef(minWidth = 25, maxWidth = 50, name = "dist"),
                  GOLD = colDef(minWidth = 100, name = "HISCO term", align = "right", filterable = TRUE),
                  thisone = colDef(
                    name = "",
                    minWidth = 75, maxWidth = 100,
                    sortable = FALSE,
                    cell = function() htmltools::tags$button("Correct")
                  )
                ),
                defaultColDef = colDef(minWidth = 100, align = "right"),
                sortable = TRUE, filterable = FALSE, searchable = FALSE, resizable = TRUE, 
                showPageSizeOptions = TRUE, pageSizeOptions = c(3, 5, 10, 15, 20, 50, 100, 1000), defaultPageSize = 10,
                borderless = TRUE,
                onClick = JS("function(rowInfo, colInfo) {
                if (colInfo.id !== 'thisone') {
                  return
                }
                field = '.row'
                if (window.Shiny) {
                  Shiny.setInputValue('save_row', { index: rowInfo.row[field]}, { priority: 'event' })
                }}"),
                theme = reactableTheme(
                  rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
                ))
    })
    observeEvent(input$ui_toon_werk, {
      x <- read_db("select * from hisco_matched")
      if(nrow(x) > 0){
        x <- x[order(x$now, decreasing = TRUE), ]
        showModal(modalDialog(title = "Reeds gevalideerd", reactable(x,
                                        columns = list(file = colDef(show = FALSE), 
                                                       .rowid = colDef(show = FALSE), 
                                                       ID_GOLD = colDef(show = FALSE),
                                                       now = colDef(show = FALSE, name = "at", format = colFormat(datetime = TRUE, locales = "nl-NL")),
                                                       skipped = colDef(cell = function(value) {
                                                         # Render as an X mark or check mark
                                                         if (value == 1) "\u274c Skipped" else "\u2714\ufe0f Validated"
                                                       }),
                                                       GOLD = colDef(name = "HISCO")),
                                        sortable = TRUE, filterable = TRUE, searchable = TRUE, resizable = TRUE, 
                                        showPageSizeOptions = TRUE, pageSizeOptions = c(3, 5, 7, 10, 15, 20, 50, 100, 1000), defaultPageSize = 7,
                                        borderless = TRUE,
                                        theme = reactableTheme(
                                          rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
                                        )), size = "xl", easyClose = TRUE))
      }
    })
    observeEvent(input$ui_skip, {
      x <- get_next()  
      n <- nrow(x)
      if(n > 0){
        x$now  <- as.character(rep(Sys.time(), nrow(x)) )
        x$file <- rep(dashdata$uploaded , nrow(x))
        x$skipped <- rep(TRUE, nrow(x))
        x <- head(x, n = 1)
        x <- x[, c("file", "now", ".rowid", ".TEXT", "ID_GOLD", "GOLD", "skipped")]
        write_db(name = "hisco_matched", value = x, overwrite = FALSE, append = TRUE)
        done_manually(done_manually() + 1)
      }
    })
    observe({
      i <- input$save_row
      removeModal()
      isolate({
        x <- get_next()  
        x <- x[i$index, ]
        if(nrow(x) == 1){
          x$now  <- as.character(rep(Sys.time(), nrow(x)))
          x$file <- rep(dashdata$uploaded , nrow(x))
          x$skipped <- rep(FALSE, nrow(x))
          x <- x[, c("file", "now", ".rowid", ".TEXT", "ID_GOLD", "GOLD", "skipped")]
          write_db(name = "hisco_matched", value = x, overwrite = FALSE, append = TRUE)
          show_alert(title = "Ok", text = paste(x$.TEXT, "=", x$GOLD))
          done_manually(done_manually() + 1)
        }
      })
    })
    dlg <- modalDialog(title = "Zoek in HISCO (wacht tot data verschijnt)", reactableOutput(outputId = "uo_zoektabel"))
    observeEvent(input$ui_zoek, {
      zoekterm <- input$ui_zoek
      zoekterm <- trimws(zoekterm)
      if(!is.null(zoekterm) && nchar(zoekterm) > 0){
        x <- get_next()
        cls <- colnames(x)
        cls <- setNames(lapply(cls, FUN = function(x) colDef(show = FALSE)), cls)
        cls$ID_GOLD <- colDef(show = TRUE, minWidth = 50, name = "ID_GOLD", align = "right")
        cls$GOLD <- colDef(show = TRUE, minWidth = 100, name = "HISCO term", align = "right", filterable = TRUE)
        cls$thisone <- colDef(show = TRUE, 
                              name = "",
                              minWidth = 75, maxWidth = 100,
                              sortable = FALSE,
                              cell = function() htmltools::tags$button("Correct")
        )
        if(nrow(x) > 0 & nchar(zoekterm) > 0){
          x <- subset(x, grepl(GOLD, pattern = zoekterm, ignore.case = TRUE))  
        }
        showModal(dlg)
        output$uo_zoektabel <- renderReactable(reactable(x,
                                                         columns = cls,
                                                         sortable = TRUE, filterable = FALSE, searchable = FALSE, resizable = TRUE,
                                                         showPageSizeOptions = TRUE, pageSizeOptions = c(3, 5, 10, 15, 20, 50, 100, 1000), defaultPageSize = 10,
                                                         borderless = TRUE,
                                                         onClick = JS("function(rowInfo, colInfo) {
              if (colInfo.id !== 'thisone') {
                return
              }
              field = '.row'
                if (window.Shiny) {
                  Shiny.setInputValue('save_row', { index: rowInfo.row[field]}, { priority: 'event' })
                }}"),
                                                         theme = reactableTheme(
                                                           rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
                                                         )))
      }
    })
    output$uo_stats_matching_manual <- renderUI({
      descriptionBlock(
        #number = done$manually,
        #numberColor = "success",
        header = done_manually(),
        text = "Manueel gevalideerd",
        rightBorder = TRUE,
        marginBottom = FALSE
      )
    })
    output$uo_corrigeer <- renderReactable({
      hisco   <- DB_HISCO()
      matched <- DB_matched()
      reactable(matched)
    })
    output$uo_stats_matching_percent <- renderUI({
      x <- matching_data()
      matched <- matcher_exact()
      aantal_exact <- sum(!is.na(matched$exact$HISCO))
      descriptionBlock(
        number = paste(round(100 * aantal_exact / nrow(x$userdata$data), 1), "%", sep = ""),
        numberColor = "success",
        #numberIcon = icon("caret-up"),
        header = aantal_exact,
        text = "Aantal exact gematcht",
        rightBorder = TRUE,
        marginBottom = FALSE
      )
    })
    ##################################################################################v
    ## UI of exporting to XLSX
    ##
    outputdataset <- reactive({
      matched          <- matcher_exact()
      matched_nonexact <- read_db("select * from hisco_matched where skipped = 0")
      if(nrow(matched_nonexact) > 0){
        matched_nonexact <- merge(matched_nonexact, dashdata$GOLD, by.x = "ID_GOLD", by.y = "ID_GOLD", all.x = TRUE, all.y = FALSE, suffixes = c("", "_gold"))
        matched_nonexact <- subset(matched_nonexact, matched_nonexact$file %in% dashdata$uploaded)
        matched_nonexact <- matched_nonexact[order(matched_nonexact$now, decreasing = TRUE), ]
        matched_nonexact <- matched_nonexact[!duplicated(matched_nonexact$.rowid), ]
      }
      m <- list(HISCO = dashdata$HISCO, GOLD = dashdata$GOLD)
      if(is.data.frame(matched$exact) && nrow(matched$exact) > 0){
        matched <- matched$exact
        matched$.matching_logic <- ifelse(is.na(matched[[head(dashdata$HISCO_fields, n = 1)]]), NA, "exact")
        fields  <- c(uploaded_file_read()$fields, dashdata$HISCO_fields)
        fields  <- intersect(colnames(matched), fields)
        if(nrow(matched_nonexact) > 0){
          ## If not exact, getting the manual assignments + if the text was already done (no .rowid, get it based on the .TEXT)
          for(field in dashdata$HISCO_fields){
            matched[[field]] <- ifelse(is.na(matched[[field]]), 
                                       txt_recode(x = matched$.rowid, from = matched_nonexact$.rowid, to = matched_nonexact[[field]], na.rm = TRUE), 
                                       matched[[field]])
            matched[[field]] <- ifelse(is.na(matched[[field]]), 
                                       txt_recode(x = matched$.TEXT, from = matched_nonexact$.TEXT, to = matched_nonexact[[field]], na.rm = TRUE), 
                                       matched[[field]])
          }
          matched$.matching_logic <- ifelse(is.na(matched$.matching_logic), 
                                            ifelse(matched$.rowid %in% matched_nonexact$.rowid, "inexact", matched$.matching_logic), 
                                            matched$.matching_logic)
        }
        matched <- matched[, c(fields, ".matching_logic")]
        #print(list(class(matched), class(dashdata$HISCO), class(dashdata$GOLD)))
        if(nrow(matched) > 0){
          m$MATCHED <- matched
        }
      }
      m <- m[intersect(c("MATCHED", "HISCO"), names(m))]
      m 
    })
    output$uo_download_results <- downloadHandler(
      filename = function() {
        paste('hisco_matcher_', Sys.Date(), '.xlsx', sep = '')
      },
      content = function(con) {
        d <- outputdataset()
        showModal(modalDialog("Data wordt in 1 excel file gestopt, ogenblikje geduld. Deze popup sluit automatisch wanneer dit afgerond is.", easyClose = FALSE, footer = NULL))
        on.exit(removeModal())
        d <- lapply(d, FUN = function(x){
          x <- setDF(x)
          fields <- sapply(x, is.character)
          x[, fields] <- apply(x[, fields, drop = FALSE], MARGIN = 2, FUN = function(x) substr(x, start = 1, stop = 32766))
          x
        })
        write_xlsx(d, path = con)
      }
    )
  }
)