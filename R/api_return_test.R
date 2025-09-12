library(tidyverse)

dt <- read_csv("./data/test_data_20250910.csv", name_repair = "universal")

dt |> 
  group_by(Area.Type) |> 
  summarise(count = n())


b <-
  dt |> 
  filter(Area.Type == "GPs") |> 
  group_by(Parent.Name, Area.Code, Area.Name) |> 
  summarise(count = n())


b |> 
  group_by(Parent.Name) |> 
  summarise(count = n())

gps <- read_csv("./data/gps.csv")
org_lkp <- read_csv("./data/orgs_lkp.csv")


c <- 
  b |> 
  inner_join(gps, by = c("Area.Code" = "GPPracticeCode_Original"))
