library(shiny)
library(bs4Dash)
library(shinydisconnect)
library(shinyglide)
library(shinyFiles)
library(readxl)
library(Microsoft365R)
library(tools)
library(udpipe)
library(data.table)
library(DT)
library(RSQLite)
library(reactable)

engine_db <- function(..., DB = settings$DB, FUN = dbListTables){
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
    data.frame()
  }
}
read_hisco <- function(){
  read_db("select * from hisco_beroepen")
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

settings <- list()
settings$DB <- R_user_dir(package = "hisco", which = "data")
if(!dir.exists(settings$DB)) dir.create(settings$DB, recursive = TRUE)
settings$url_hisco            <- "https://iisg.amsterdam/en/data/data-websites/history-of-work"
settings$url_hisco_data       <- "https://datasets.iisg.amsterdam/dataset.xhtml?persistentId=hdl:10622/MUZMAL"
load(url("https://github.com/DIGI-VUB/hiscomatcher/raw/master/data/hisco.RData"))
settings$HISCO_fields <- c("ID", "Original", "Standard", "HISCO", "STATUS", "RELATION", 
                           "PRODUCT", "HISCLASS", "HISCLASS_5", "HISCAM_U1", "HISCAM_NL", 
                           "SOCPO", "OCC1950", "Release")
hisco$activiteit_standard  <- hisco$Standard
hisco$activiteit           <- hisco$Original
hisco$activiteit_standard_cleaned  <- txt_standardiser(hisco$activiteit_standard)
hisco$activiteit_cleaned           <- txt_standardiser(hisco$activiteit)

settings$GOLD <- paste.data.frame(data = hisco, term = c("activiteit", "activiteit_cleaned", "activiteit_standard"), 
                                  group = c("activiteit_standard_cleaned", "HISCO", "STATUS", "RELATION", 
                                            "PRODUCT", "HISCLASS", "HISCLASS_5", "HISCAM_U1", "HISCAM_NL", 
                                            "SOCPO", "OCC1950", "Release"), 
                                  collapse = "|")

settings$DB    <- file.path(settings$DB, "hisco.sqlite")
settings$HISCO <- hisco
write_db(name = "hisco", value = settings$HISCO, overwrite = TRUE)
write_db(name = "hisco_beroepen", value = settings$GOLD, overwrite = TRUE)

mod_start <- modalDialog(
  title = "Welkom",
  tags$p("Het objectief van deze app is het matchen van beroepen aan de ", tags$a("HISCO", href = settings$url_hisco), " dataset om een gestandaardiseerde set te bekomen van beroepen. 
  Hoe doe je dit?"),
  tags$ul(
    tags$li("Laadt je data met beroepen op in EXCEL/CSV formaat"),
    tags$li("Er gebeurt een automatische match"),
    tags$li("Je valideert de matches")
  ),
  "De gestandaardiseerde set wordt gesaved en is te vinden op volgende online link.",
  easyClose = TRUE,
  footer = NULL
)
data_reader <- glide(
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
)



shinyApp(
  ui = dashboardPage(
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
          text = "DATA",
          icon = icon("cubes"),
          startExpanded = TRUE,
          menuSubItem(
            text = HTML(
              paste(
                "Export",
                dashboardBadge(
                  ">>",
                  position = "right",
                  color = "success"
                )
              )
            ),
            tabName = "tab_export",
            icon = icon("circle")
          ),
          menuSubItem(
            text = HTML("HISCO Categorieen"),
            tabName = "tab_hisco_categories",
            icon = icon("circle")
            
          )
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
                fluidPage(title = "TEST",
                          fluidRow(
                            column(
                              width = 8,
                              tabBox(
                                ribbon(
                                  text = "Matching",
                                  color = "pink"
                                ),
                                elevation = 2,
                                id = "tabcard1",
                                width = 12,
                                collapsible = FALSE, 
                                closable = FALSE,
                                type = "tabs",
                                status = "primary",
                                solidHeader = TRUE,
                                selected = "Valideer",
                                tabPanel(title = "Valideer",
                                         reactableOutput(outputId = "uo_valideer")
                                         
                                ),
                                tabPanel(title = "Corrigeer",
                                         reactableOutput(outputId = "uo_corrigeer")
                                )
                              )
                            ),
                            column(width = 4,
                                   box(
                                     solidHeader = FALSE,
                                     title = "Data",
                                     width = 12,
                                     status = "success",
                                     footer = fluidRow(
                                       column(
                                         width = 6,
                                         uiOutput(outputId = "uo_stats_records_own")
                                       ),
                                       column(
                                         width = 6,
                                         uiOutput(outputId = "uo_stats_records_hisco")
                                       )
                                     )
                                   ),
                                   box(
                                     solidHeader = FALSE,
                                     title = "Match progress",
                                     background = NULL,
                                     width = 12,
                                     status = "info",
                                     footer = fluidRow(
                                       column(
                                         width = 6,
                                         uiOutput(outputId = "uo_stats_matching_percent")
                                       ),
                                       column(
                                         width = 6,
                                         uiOutput(outputId = "uo_stats_matching_todo")
                                       )
                                     )
                                   ))  
                          )
                          )),
        tabItem(tabName = "tab_upload", 
                fluidRow(
                  column(width = 2),
                  column(width = 8, data_reader),
                  column(width = 2)  
                ))
      )
    ),
    # controlbar = dashboardControlbar(
    #   id = "controlbar",
    #   skin = "light",
    #   pinned = TRUE,
    #   overlay = FALSE,
    #   controlbarMenu(
    #     id = "controlbarMenu",
    #     type = "pills",
    #     controlbarItem(
    #       "Inputs",
    #       column(
    #         width = 12,
    #         align = "center",
    #         radioButtons(
    #           inputId = "dist",
    #           label = "Distribution type:",
    #           c(
    #             "Normal" = "norm",
    #             "Uniform" = "unif",
    #             "Log-normal" = "lnorm",
    #             "Exponential" = "exp"
    #           )
    #         )
    #       )
    #     )
    #   )
    # ),
    footer = dashboardFooter(
      fixed = FALSE,
      left = a(
        href = "https://digi.research.vub.be/",
        target = "_blank", "DIGI VUB"
      ),
      right = "2022"
    ),
    title = "bs4Dash Showcase"
  ),
  server = function(input, output, session) {
    showModal(mod_start)
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
      list(data = d, fields = colnames(d), field_content = tail(head(colnames(d), n = 2), n = 1))
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
          tags$blockquote(icon("columns"), "Selecteer een aantal velden om te matchen met HISCO"),
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
    DB_HISCO <- reactive({
      input$ui_upload_file
      #ds <- settings$HISCO
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
      # valueBox(
      #   value = n,
      #   subtitle = "Aantal records in HISCO",
      #   color = "primary",
      #   icon = icon("cogs"),
      #   href = settings$url_hisco_data,
      #   #footer = tags$a("HISCO", href = settings$url_hisco_data),
      #   width = 12
      # )
      #♠infoBox(title = "Aantal records", subtitle = "in jouw dataset", value = n, color = "info", icon = icon("database"), width = 12, elevation = 4)
    })
    output$uo_valideer <- renderReactable({
      reactable(iris)
    })
    output$uo_corrigeer <- renderReactable({
      hisco   <- DB_HISCO()
      matched <- DB_matched()
      reactable(matched)
    })
    output$uo_stats_matching_percent <- renderUI({
      ds <- uploaded_file_read()
      descriptionBlock(
        number = "18%",
        numberColor = "danger",
        numberIcon = icon("caret-down"),
        header = "1200",
        text = "Aantal gematcht",
        rightBorder = FALSE,
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