library(shiny)
library(data.table)
library(ggplot2)
library(DT)
library(rmongodb)


# Define UI for application that draws a histogram
ui <- fluidPage(
  
  # Application title
  fluidRow(
    column(3, titlePanel("TTT Link Viewer"))
    ),
  
  
  fluidRow(
    column(3, uiOutput("select_result")),
    column(3, uiOutput("source_file_select")),
    column(3, sliderInput("slope",
                          "Confidence Slope:",
                          min = 0.05,
                          max = 4,
                          value = 1.9,
                          step = 0.05)),
    column(3, sliderInput("centrepoint",
                          "Confidence Centre:",
                          min = 1,
                          max = 12,
                          value = 5.8,
                          step = 0.1))#,
  ),
  fluidRow(
    column(1, checkboxInput("show_series",
                            "Toggle Series")),
    column(6, offset = 1,
           sliderInput("range",
                       "Score Range:",
                       width = 700,
                       min = -10,
                       max = 40,
                       value = c(5,25),
                       step = 0.2)),
    column(4, sliderInput("records",
                          "Results Count:",
                          min = 10,
                          max = 5000,
                          value = 100,
                          step = 10))
  ) ,


    mainPanel(
      tabsetPanel(type = "tabs", 
                  tabPanel("Details", p(h3(textOutput("link_count"))), tableOutput("audittrail")),
                  tabPanel("Graph", plotOutput("summary")),
                  tabPanel("Different Surnames", DT::dataTableOutput("diffsurs"),verbatimTextOutput('diffsur_scores')),
                  tabPanel("Different Forenames", DT::dataTableOutput("difffores"), verbatimTextOutput('difffores_scores')),
                  tabPanel("Different Sample", DT::dataTableOutput("diffsample"), verbatimTextOutput('diffsample_scores')),
                  tabPanel("Different DOBs", DT::dataTableOutput("diffdobs"), verbatimTextOutput('diffdobs_scores')),
                  tabPanel("Random Sample", DT::dataTableOutput("randomsample"), verbatimTextOutput('diffrand_scores')),
                  tabPanel("Different Serv Nums", DT::dataTableOutput("diffsnums"), verbatimTextOutput('diffsnums_scores')),
                  tabPanel("What is this?", includeHTML("./instructions.txt"))
      )
    , width = "100%")
  )
#)

