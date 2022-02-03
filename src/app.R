library(shiny)
library(bs4Dash)
library(shinydisconnect)
library(shinyglide)
library(shinyFiles)
library(shinyWidgets)
library(readxl)
library(tools)
library(udpipe)
library(data.table)
library(DT)
library(RSQLite)
library(reactable)

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
txt_standardiser <- function(x){
  ## Put all in ASCII, lowercase, keep only letters and spaces, no special spaces/punctuation symbols
  x <- iconv(x, to = "ASCII//TRANSLIT")
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
dashdata$HISCO$activiteit_standard_cleaned  <- txt_standardiser(dashdata$HISCO$activiteit_standard)
dashdata$HISCO$activiteit_cleaned           <- txt_standardiser(dashdata$HISCO$activiteit)
dashdata$HISCO$ID_GOLD <- as.integer(factor(dashdata$HISCO$activiteit_standard_cleaned))
dashdata$GOLD <- paste.data.frame(data = dashdata$HISCO, term = c("activiteit", "activiteit_cleaned", "activiteit_standard", "Original", "Standard"), 
                                  group = c("ID_GOLD", "activiteit_standard_cleaned", "HISCO", "STATUS", "RELATION", 
                                            "PRODUCT", "HISCLASS", "HISCLASS_5", "HISCAM_U1", "HISCAM_NL", 
                                            "SOCPO", "OCC1950", "Release"), 
                                  collapse = "|")

## Save initial values to the database
write_db(name = "hisco", value = dashdata$HISCO, overwrite = TRUE)
write_db(name = "hisco_gold", value = dashdata$GOLD, overwrite = TRUE)

x <- read_hisco()
x <- as.data.table(x[, c("ID_GOLD", "activiteit_cleaned")])
x <- x[, lapply(.SD, unlist), by = list(ID_GOLD), .SDcols = "activiteit_cleaned"]
x <- subset(x)

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
    ## exact match on standard 
    x$.match_standard <- txt_recode(x[[fields]], from = GOLD$activiteit_standard_cleaned, to = GOLD$ID_GOLD, na.rm = TRUE)
    ## exact match on all variants
    x$.match_variant  <- txt_recode(x[[fields]], from = GOLD_act$activiteit_cleaned, to = GOLD_act$ID_GOLD, na.rm = TRUE)
    x$.match          <- ifelse(is.na(x$.match), ifelse(is.na(x$.match_standard), x$.match_variant, x$.match_standard), x$.match)
  }
  x$.MATCH <- txt_recode(x$.match, from = VARIANTS$ID_GOLD, to = VARIANTS$Standard, na.rm = TRUE)
  x <- merge(x, GOLD, by.x = ".match", by.y = "ID_GOLD", sort = FALSE, all.x = TRUE)
  x <- x[order(x$.rowid, decreasing = FALSE), ]
  x
}


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
        )
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
                                         tags$blockquote("Deze dataset toont termen die niet 100% exact konden gematcht worden en dus validatie vereisen"),
                                         reactableOutput(outputId = "uo_valideer")
                                ),
                                tabPanel(title = "Corrigeer",
                                         reactableOutput(outputId = "uo_corrigeer")
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
                                       #column(
                                       #  width = 6,
                                       #   uiOutput(outputId = "uo_stats_matching_todo")
                                       #)
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
            sliderInput(inputId = "ui_matching_stringdist_topn", min = 1, max = 25, value = 7, label = "Toon top-n matches")
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
    uploaded_file <- reactive({
      path <- parseFilePaths(dirs, input$ui_upload_file)$datapath
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
      top_n <- input$ui_matching_stringdist_topn 
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
    matcher <- reactive({
      matchinfo <- uploaded_file_matchinfo()
      hisco     <- DB_HISCO()
      x         <- match_exact(matchinfo$userdata$data, GOLD = hisco, fields = matchinfo$match_on)
      list(exact = x, hisco = hisco, matchinfo = matchinfo)
    })
    output$uo_exact <- renderReactable({
      ds <- matcher()
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
        reactable(x, 
                  sortable = TRUE, filterable = TRUE, searchable = TRUE, resizable = TRUE, 
                  showPageSizeOptions = TRUE, pageSizeOptions = c(3, 5, 10, 15, 20, 50, 100, 1000), defaultPageSize = 5,
                  borderless = TRUE,
                  groupBy = groupby, defaultColDef = colDef(aggregate = "unique"),
                  columnGroups = list(
                    colGroup(name = "MATCH", columns = ".MATCH", align = "left", headerStyle = list(fontWeight = 700)),
                    colGroup(name = "Uw data", columns = matchinfo$userdata$fields, align = "left", headerStyle = list(fontWeight = 700)),
                    colGroup(name = "HISCO data", columns = show_hisco_fields, align = "left", headerStyle = list(fontWeight = 700))
                  )) 
      }
    })
    output$uo_valideer <- renderReactable({
      reactable(iris,
                sortable = TRUE, filterable = TRUE, searchable = TRUE, resizable = TRUE, 
                showPageSizeOptions = TRUE, pageSizeOptions = c(3, 5, 10, 15, 20, 50, 100, 1000), defaultPageSize = 10,
                borderless = TRUE,
                selection = "multiple",
                onClick = "select", 
                theme = reactableTheme(
                  rowSelectedStyle = list(backgroundColor = "#eee", boxShadow = "inset 2px 0 0 0 #ffa62d")
                ))
    })
    output$uo_corrigeer <- renderReactable({
      hisco   <- DB_HISCO()
      matched <- DB_matched()
      reactable(matched)
    })
    output$uo_stats_matching_percent <- renderUI({
      x <- matching_data()
      matched <- matcher()
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
    shinyFileChoose(input, id = 'ui_upload_file', roots = dirs, filetypes = c('csv', 'xlsx', 'xls'))
    observeEvent(input$current_tab, {
      if (input$current_tab == "tab_upload") {
      }
    })
  }
)