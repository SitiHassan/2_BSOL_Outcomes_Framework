# calculate_dsr2 ---------------------------------------------------------------
# This function ONLY allows the multiplier argument to be passed as a scalar value

calculate_dsr2 <-
  function (data, x, n, stdpop = NULL, type = "full", confidence = 0.95,
            multiplier = 1e+05, independent_events = TRUE, eventfreq = NULL,
            ageband = NULL) {
    if (missing(data) | missing(x) | missing(n) | missing(stdpop)) {
      stop("function calculate_dsr requires at least 4 arguments: data, x, n, stdpop")
    }
    if (!is.data.frame(data)) {
      stop("data must be a data frame object")
    }
    if (!deparse(substitute(x)) %in% colnames(data)) {
      stop("x is not a field name from data")
    }
    if (!deparse(substitute(n)) %in% colnames(data)) {
      stop("n is not a field name from data")
    }
    if (!deparse(substitute(stdpop)) %in% colnames(data)) {
      stop("stdpop is not a field name from data")
    }
    data <- data %>% rename(x = {
      {
        x
      }
    }, n = {
      {
        n
      }
    }, stdpop = {
      {
        stdpop
      }
    })
    if (!is.numeric(data$x)) {
      stop("field x must be numeric")
    }
    else if (!is.numeric(data$n)) {
      stop("field n must be numeric")
    }
    else if (!is.numeric(data$stdpop)) {
      stop("field stdpop must be numeric")
    }
    else if (anyNA(data$n)) {
      stop("field n cannot have missing values")
    }
    else if (anyNA(data$stdpop)) {
      stop("field stdpop cannot have missing values")
    }
    else if (any(pull(data, x) < 0, na.rm = TRUE)) {
      stop("numerators must all be greater than or equal to zero")
    }
    else if (any(pull(data, n) <= 0)) {
      stop("denominators must all be greater than zero")
    }
    else if (any(pull(data, stdpop) < 0)) {
      stop("stdpop must all be greater than or equal to zero")
    }
    else if (!(type %in% c("value", "lower", "upper",
                           "standard", "full"))) {
      stop("type must be one of value, lower, upper, standard or full")
    }
    else if (!is.numeric(confidence)) {
      stop("confidence must be numeric")
    }
    else if (length(confidence) > 2) {
      stop("a maximum of two confidence levels can be provided")
    }
    else if (length(confidence) == 2) {
      if (!(confidence[1] == 0.95 & confidence[2] == 0.998)) {
        stop("two confidence levels can only be produced if they are specified as 0.95 and 0.998")
      }
    }
    else if ((confidence < 0.9) | (confidence > 1 & confidence <
                                   90) | (confidence > 100)) {
      stop("confidence level must be between 90 and 100 or between 0.9 and 1")
    }
    else if (!is.numeric(multiplier)) {
      stop("multiplier must be numeric")
    }
    else if (multiplier <= 0) {
      stop("multiplier must be greater than 0")
    }
    else if (!rlang::is_bool(independent_events)) {
      stop("independent_events must be TRUE or FALSE")
    }
    if (!independent_events) {
      if (missing(eventfreq)) {
        stop(paste0("function calculate_dsr requires an eventfreq column ",
                    "to be specified when independent_events is FALSE"))
      }
      else if (!deparse(substitute(eventfreq)) %in% colnames(data)) {
        stop("eventfreq is not a field name from data")
      }
      else if (!is.numeric(data[[deparse(substitute(eventfreq))]])) {
        stop("eventfreq field must be numeric")
      }
      else if (anyNA(data[[deparse(substitute(eventfreq))]])) {
        stop("eventfreq field must not have any missing values")
      }
      if (missing(ageband)) {
        stop(paste0("function calculate_dsr requires an ageband column ",
                    "to be specified when independent_events is FALSE"))
      }
      else if (!deparse(substitute(ageband)) %in% colnames(data)) {
        stop("ageband is not a field name from data")
      }
      else if (anyNA(data[[deparse(substitute(ageband))]])) {
        stop("ageband field must not have any missing values")
      }
    }
    if (independent_events) {
      dsrs <- dsr_inner2(data = data, x = x, n = n, stdpop = stdpop,
                         type = type, confidence = confidence, multiplier = multiplier)
    }
    else {
      data <- data %>% rename(eventfreq = {
        {
          eventfreq
        }
      }, ageband = {
        {
          ageband
        }
      }) %>% group_by(eventfreq, .add = TRUE)
      grps <- group_vars(data)[!group_vars(data) %in% "eventfreq"]
      check_groups <- filter(summarise(group_by(data, pick(all_of(c(grps,
                                                                    "ageband")))), num_n = n_distinct(.data$n),
                                       num_stdpop = n_distinct(.data$stdpop), .groups = "drop"),
                             .data$num_n > 1 | .data$num_stdpop > 1)
      if (nrow(check_groups) > 0) {
        stop(paste0("There are rows with the same grouping variables and ageband",
                    " but with different populations (n) or standard populations",
                    "(stdpop)"))
      }
      freq_var <- data %>% dsr_inner2(x = x, n = n, stdpop = stdpop,
                                      type = type, confidence = confidence, multiplier = multiplier,
                                      rtn_nonindependent_vardsr = TRUE) %>% mutate(freqvars = .data$vardsr *
                                                                                     .data$eventfreq^2) %>% group_by(pick(all_of(grps))) %>%
        summarise(custom_vardsr = sum(.data$freqvars), .groups = "drop")
      event_data <- data %>% mutate(events = .data$eventfreq *
                                      .data$x) %>% group_by(pick(all_of(c(grps, "ageband",
                                                                          "n", "stdpop")))) %>% summarise(x = sum(.data$events,
                                                                                                                  na.rm = TRUE), .groups = "drop")
      dsrs <- event_data %>% left_join(freq_var, by = grps) %>%
        group_by(pick(all_of(grps))) %>% dsr_inner2(x = x,
                                                    n = n, stdpop = stdpop, type = type, confidence = confidence,
                                                    multiplier = multiplier, use_nonindependent_vardsr = TRUE)
    }
    return(dsrs)
  }



# calculate_dsr3 ---------------------------------------------------------------
# This function allows the multiplier argument to be passed as a column name

calculate_dsr3 <-
  function (data, x, n, stdpop = NULL, type = "full", confidence = 0.95,
            multiplier = 1e+05, independent_events = TRUE, eventfreq = NULL,
            ageband = NULL) {

    # ---- basic argument checks ----
    if (missing(data) | missing(x) | missing(n) | missing(stdpop)) {
      stop("function calculate_dsr requires at least 4 arguments: data, x, n, stdpop")
    }
    if (!is.data.frame(data)) stop("data must be a data frame object")
    if (!deparse(substitute(x)) %in% colnames(data)) stop("x is not a field name from data")
    if (!deparse(substitute(n)) %in% colnames(data)) stop("n is not a field name from data")
    if (!deparse(substitute(stdpop)) %in% colnames(data)) stop("stdpop is not a field name from data")

    # ---- standardise core columns ----
    data <- dplyr::rename(
      data,
      x      = {{ x }},
      n      = {{ n }},
      stdpop = {{ stdpop }}
    )

    if (!is.numeric(data$x))      stop("field x must be numeric")
    else if (!is.numeric(data$n)) stop("field n must be numeric")
    else if (!is.numeric(data$stdpop)) stop("field stdpop must be numeric")
    else if (anyNA(data$n))       stop("field n cannot have missing values")
    else if (anyNA(data$stdpop))  stop("field stdpop cannot have missing values")
    else if (any(dplyr::pull(data, x) < 0, na.rm = TRUE)) stop("numerators must all be >= 0")
    else if (any(dplyr::pull(data, n) <= 0))              stop("denominators must all be > 0")
    else if (any(dplyr::pull(data, stdpop) < 0))          stop("stdpop must all be >= 0")
    else if (!(type %in% c("value", "lower", "upper", "standard", "full")))
      stop("type must be one of value, lower, upper, standard or full")
    else if (!is.numeric(confidence))
      stop("confidence must be numeric")
    else if (length(confidence) > 2)
      stop("a maximum of two confidence levels can be provided")
    else if (length(confidence) == 2) {
      if (!(confidence[1] == 0.95 & confidence[2] == 0.998)) {
        stop("two confidence levels can only be produced if they are specified as 0.95 and 0.998")
      }
    } else if ((confidence < 0.9) | (confidence > 1 & confidence < 90) | (confidence > 100)) {
      stop("confidence level must be between 90 and 100 or between 0.9 and 1")
    }

    # ---- allow multiplier to be a column OR a scalar ----
    mult_is_col <- deparse(substitute(multiplier)) %in% colnames(data)

    if (mult_is_col) {
      data <- dplyr::rename(data, multiplier = {{ multiplier }})
      if (!is.numeric(data$multiplier)) stop("multiplier column must be numeric")
      if (anyNA(data$multiplier))       stop("multiplier column cannot have missing values")
      if (any(data$multiplier <= 0))    stop("multiplier column values must be > 0")
    } else {
      # scalar path -> inject as a column so downstream is uniform
      if (!is.numeric(multiplier)) stop("multiplier must be numeric")
      if (length(multiplier) != 1) stop("multiplier must be length 1 when not a column")
      if (multiplier <= 0)         stop("multiplier must be greater than 0")
      data <- dplyr::mutate(data, multiplier = multiplier)
    }

    # ---- handle independent / non-independent events ----
    if (!rlang::is_bool(independent_events)) {
      stop("independent_events must be TRUE or FALSE")
    }

    if (independent_events) {
      dsrs <- dsr_inner2(
        data = data, x = x, n = n, stdpop = stdpop,
        type = type, confidence = confidence,
        rtn_nonindependent_vardsr = FALSE,
        use_nonindependent_vardsr = FALSE
      )
    } else {
      # validate eventfreq and ageband
      if (missing(eventfreq)) {
        stop("function calculate_dsr requires an eventfreq column when independent_events is FALSE")
      } else if (!deparse(substitute(eventfreq)) %in% colnames(data)) {
        stop("eventfreq is not a field name from data")
      } else if (!is.numeric(data[[deparse(substitute(eventfreq))]])) {
        stop("eventfreq field must be numeric")
      } else if (anyNA(data[[deparse(substitute(eventfreq))]])) {
        stop("eventfreq field must not have any missing values")
      }

      if (missing(ageband)) {
        stop("function calculate_dsr requires an ageband column when independent_events is FALSE")
      } else if (!deparse(substitute(ageband)) %in% colnames(data)) {
        stop("ageband is not a field name from data")
      } else if (anyNA(data[[deparse(substitute(ageband))]])) {
        stop("ageband field must not have any missing values")
      }

      data <- data %>%
        dplyr::rename(
          eventfreq = {{ eventfreq }},
          ageband   = {{ ageband }}
        ) %>%
        dplyr::group_by(eventfreq, .add = TRUE)

      grps <- dplyr::group_vars(data)[!dplyr::group_vars(data) %in% "eventfreq"]

      check_groups <- dplyr::group_by(data, dplyr::pick(dplyr::all_of(c(grps, "ageband")))) %>%
        dplyr::summarise(
          num_n      = dplyr::n_distinct(.data$n),
          num_stdpop = dplyr::n_distinct(.data$stdpop),
          .groups = "drop"
        ) %>%
        dplyr::filter(.data$num_n > 1 | .data$num_stdpop > 1)

      if (nrow(check_groups) > 0) {
        stop(paste0("There are rows with the same grouping variables and ageband",
                    " but with different populations (n) or standard populations (stdpop)"))
      }

      # vardsr under non-independent events (multiplier not required here)
      freq_var <- data %>%
        dsr_inner2(
          x = x, n = n, stdpop = stdpop, type = type,
          confidence = confidence,
          rtn_nonindependent_vardsr = TRUE,
          use_nonindependent_vardsr = FALSE
        ) %>%
        dplyr::mutate(freqvars = .data$vardsr * .data$eventfreq^2) %>%
        dplyr::group_by(dplyr::pick(dplyr::all_of(grps))) %>%
        dplyr::summarise(custom_vardsr = sum(.data$freqvars), .groups = "drop")

      # collapse events and retain multiplier per group
      event_data <- data %>%
        dplyr::mutate(events = .data$eventfreq * .data$x) %>%
        dplyr::group_by(dplyr::pick(dplyr::all_of(c(grps, "ageband", "n", "stdpop")))) %>%
        dplyr::summarise(
          x = sum(.data$events, na.rm = TRUE),
          multiplier = dplyr::first(.data$multiplier),
          .groups = "drop"
        )

      dsrs <- event_data %>%
        dplyr::left_join(freq_var, by = grps) %>%
        dplyr::group_by(dplyr::pick(dplyr::all_of(grps))) %>%
        dsr_inner2(
          x = x, n = n, stdpop = stdpop, type = type,
          confidence = confidence,
          rtn_nonindependent_vardsr = FALSE,
          use_nonindependent_vardsr = TRUE
        )
    }

    return(dsrs)
  }

# dsr_inner2 --------------------------------------------------------------------
dsr_inner2 <-
  function (data, x, n, stdpop, type, confidence,
            rtn_nonindependent_vardsr = FALSE,
            use_nonindependent_vardsr = FALSE) {

    if (isTRUE(rtn_nonindependent_vardsr) &&
        ("custom_vardsr" %in% names(data) || isTRUE(use_nonindependent_vardsr))) {
      stop("cannot get nonindependent vardsr and use nonindependent vardsr in the same execution")
    }

    # normalise confidence inputs (allow 95 and 0.95 forms)
    confidence[confidence >= 90] <- confidence[confidence >= 90] / 100
    conf1 <- confidence[1]
    conf2 <- confidence[2]

    # ensure multiplier column present & valid
    if (!"multiplier" %in% names(data)) {
      stop("internal error: multiplier column not found. Ensure calculate_dsr2() injected/renamed it.")
    }
    if (!is.numeric(data$multiplier)) stop("multiplier column must be numeric")
    if (anyNA(data$multiplier))       stop("multiplier column cannot have missing values")
    if (any(data$multiplier <= 0))    stop("multiplier column values must be > 0")

    # multiplier must be constant within groups
    grps <- dplyr::group_vars(data)
    if (length(grps) > 0) {
      mult_check <- dplyr::summarise(data, nuniq_mult = dplyr::n_distinct(.data$multiplier), .groups = "drop")
      if (any(mult_check$nuniq_mult > 1)) stop("Within-group multiplier must be constant.")
    }

    # choose method
    if (!isTRUE(use_nonindependent_vardsr)) {
      method <- "Dobson"
      data <- dplyr::mutate(data, custom_vardsr = NA_real_)
    } else {
      method <- "Dobson, with confidence adjusted for non-independent events"
    }

    dsrs <- data %>%
      dplyr::mutate(
        wt_rate = PHEindicatormethods:::na.zero(.data$x) * .data$stdpop / .data$n,
        sq_rate = PHEindicatormethods:::na.zero(.data$x) * (.data$stdpop / (.data$n))^2
      ) %>%
      dplyr::summarise(
        total_count = sum(.data$x, na.rm = TRUE),
        total_pop   = sum(.data$n),
        base_value  = sum(.data$wt_rate) / sum(.data$stdpop),
        vardsr = dplyr::case_when(
          isTRUE(use_nonindependent_vardsr) ~ unique(.data$custom_vardsr),
          TRUE ~ 1 / sum(.data$stdpop)^2 * sum(.data$sq_rate)
        ),
        multiplier = dplyr::first(.data$multiplier),
        .groups = "keep"
      )

    if (!isTRUE(rtn_nonindependent_vardsr)) {
      dsrs <- dsrs %>%
        dplyr::ungroup() %>%
        dplyr::mutate(
          value    = .data$base_value * .data$multiplier,
          lowercl  = .data$value + sqrt(.data$vardsr/.data$total_count) *
            (PHEindicatormethods:::byars_lower(.data$total_count, conf1) - .data$total_count) *
            .data$multiplier,
          uppercl  = .data$value + sqrt(.data$vardsr/.data$total_count) *
            (PHEindicatormethods:::byars_upper(.data$total_count, conf1) - .data$total_count) *
            .data$multiplier,
          lower99_8cl = .data$value + sqrt(.data$vardsr/.data$total_count) *
            (PHEindicatormethods:::byars_lower(.data$total_count, 0.998) - .data$total_count) *
            .data$multiplier,
          upper99_8cl = .data$value + sqrt(.data$vardsr/.data$total_count) *
            (PHEindicatormethods:::byars_upper(.data$total_count, 0.998) - .data$total_count) *
            .data$multiplier,
          confidence = paste0(confidence * 100, "%", collapse = ", "),
          statistic  = paste("dsr per", format(.data$multiplier, scientific = FALSE)),
          method     = method
        )

      # rename/drops to mirror PHE functions' dual-CI behavior
      if (!is.na(conf2)) {
        names(dsrs)[names(dsrs) == "lowercl"] <- "lower95_0cl"
        names(dsrs)[names(dsrs) == "uppercl"] <- "upper95_0cl"
      } else {
        dsrs <- dplyr::select(dsrs, !c("lower99_8cl", "upper99_8cl"))
      }
    }

    if (isTRUE(rtn_nonindependent_vardsr)) {
      dsrs <- dplyr::select(dsrs, dplyr::group_cols(), "vardsr")
    } else if (type == "lower") {
      dsrs <- dplyr::select(dsrs, !c("total_count","total_pop","value",
                                     dplyr::starts_with("upper"),
                                     "vardsr","confidence","statistic","method"))
    } else if (type == "upper") {
      dsrs <- dplyr::select(dsrs, !c("total_count","total_pop","value",
                                     dplyr::starts_with("lower"),
                                     "vardsr","confidence","statistic","method"))
    } else if (type == "value") {
      dsrs <- dplyr::select(dsrs, !c("total_count","total_pop",
                                     dplyr::starts_with("lower"), dplyr::starts_with("upper"),
                                     "vardsr","confidence","statistic","method"))
    } else if (type == "standard") {
      dsrs <- dplyr::select(dsrs, !c("vardsr","confidence","statistic","method"))
    } else if (type == "full") {
      dsrs <- dplyr::select(dsrs, !c("vardsr"))
    }

    return(dsrs)
  }


