(function () {
  "use strict";

  var maxResults = 9;
  var catalog = {
    payloadFields: [
      { label: "msg.payload.heading", detail: "Heading · PGN 127250" },
      { label: "msg.payload.speedWaterReferenced", detail: "Speed Water Referenced · PGN 128259" }
    ],
    pgns: [
      { label: "127250", detail: "Vessel Heading" },
      { label: "128259", detail: "Speed" },
      { label: "129025", detail: "Position, Rapid Update" },
      { label: "129026", detail: "COG & SOG, Rapid Update" },
      { label: "129029", detail: "GNSS Position Data" }
    ]
  };
  var catalogPromise;

  var envelopeItems = [
    completion("msg.pgn", "int · NMEA 2000 PGN number", "field"),
    completion("msg.source", "int · source device address", "field"),
    completion("msg.dest", "int · destination address", "field"),
    completion("msg.priority", "int · 0 (highest) to 7 (lowest)", "field"),
    completion("msg.timestamp", "string · RFC 3339 timestamp", "field"),
    completion("msg.payload", "map · decoded PGN fields", "field")
  ];

  var languageItems = [
    completion("has()", "macro · test whether an optional field exists", "function", "has()", -1),
    completion("size()", "function · string, list, or map size", "function", "size()", -1),
    completion("int()", "function · convert a value to int", "function", "int()", -1),
    completion("double()", "function · convert a value to double", "function", "double()", -1),
    completion("string()", "function · convert a value to string", "function", "string()", -1),
    completion("true", "bool literal", "keyword"),
    completion("false", "bool literal", "keyword"),
    completion("in", "membership operator", "keyword", "in ")
  ];

  function completion(label, detail, kind, insertText, cursorOffset) {
    return {
      label: label,
      detail: detail,
      kind: kind,
      insertText: insertText || label,
      cursorOffset: cursorOffset || 0
    };
  }

  function loadCatalog() {
    if (catalogPromise) return catalogPromise;
    catalogPromise = fetch("/cel-completions", {
      headers: { Accept: "application/json" }
    })
      .then(function (response) {
        if (!response.ok) throw new Error("completion catalog unavailable");
        return response.json();
      })
      .then(function (data) {
        catalog.payloadFields = mergeCatalogItems(catalog.payloadFields, data.payloadFields);
        catalog.pgns = mergeCatalogItems(catalog.pgns, data.pgns);
        return catalog;
      })
      .catch(function () {
        return catalog;
      });
    return catalogPromise;
  }

  function mergeCatalogItems(fallback, incoming) {
    var merged = [];
    var seen = Object.create(null);
    (Array.isArray(incoming) ? incoming : fallback).forEach(function (item) {
      if (!item || !item.label || seen[item.label]) return;
      seen[item.label] = true;
      merged.push(item);
    });
    fallback.forEach(function (item) {
      if (seen[item.label]) return;
      seen[item.label] = true;
      merged.push(item);
    });
    return merged;
  }

  function initCELAutocomplete(root) {
    var scope = root && root.querySelectorAll ? root : document;
    scope.querySelectorAll("[data-cel-autocomplete]").forEach(function (editor) {
      if (editor.dataset.autocompleteReady === "true") return;
      editor.dataset.autocompleteReady = "true";
      attachAutocomplete(editor);
    });
  }

  function attachAutocomplete(editor) {
    var textarea = editor.querySelector("textarea");
    var listbox = editor.querySelector("[role=listbox]");
    var status = editor.querySelector("[data-cel-completion-status]");
    if (!textarea || !listbox || !status) return;

    var matches = [];
    var activeIndex = -1;
    var replaceStart = 0;
    var replaceEnd = 0;

    function close() {
      matches = [];
      activeIndex = -1;
      listbox.hidden = true;
      listbox.replaceChildren();
      textarea.setAttribute("aria-expanded", "false");
      textarea.removeAttribute("aria-activedescendant");
      status.textContent = "";
    }

    function setActive(index) {
      if (!matches.length) return;
      activeIndex = (index + matches.length) % matches.length;
      Array.prototype.forEach.call(listbox.children, function (option, optionIndex) {
        option.setAttribute("aria-selected", optionIndex === activeIndex ? "true" : "false");
      });
      var active = listbox.children[activeIndex];
      if (active) {
        textarea.setAttribute("aria-activedescendant", active.id);
        active.scrollIntoView({ block: "nearest" });
      }
    }

    function render(context) {
      matches = context.items.slice(0, context.limit || maxResults);
      replaceStart = context.start;
      replaceEnd = context.end;
      listbox.replaceChildren();
      if (!matches.length) {
        close();
        return;
      }

      matches.forEach(function (item, index) {
        var option = document.createElement("div");
        option.id = listbox.id + "-option-" + index;
        option.className = "cel-completion-option";
        option.setAttribute("role", "option");
        option.setAttribute("aria-selected", "false");
        option.dataset.index = String(index);

        var main = document.createElement("span");
        main.className = "cel-completion-main";
        var label = document.createElement("code");
        label.className = "cel-completion-label";
        label.textContent = item.label;
        var kind = document.createElement("span");
        kind.className = "cel-completion-kind";
        kind.textContent = item.kind;
        main.append(label, kind);

        var detail = document.createElement("span");
        detail.className = "cel-completion-detail";
        detail.textContent = item.detail;
        option.append(main, detail);

        option.addEventListener("pointermove", function () {
          setActive(index);
        });
        option.addEventListener("pointerdown", function (event) {
          event.preventDefault();
          accept(index);
        });
        listbox.appendChild(option);
      });

      listbox.hidden = false;
      textarea.setAttribute("aria-expanded", "true");
      status.textContent = matches.length + " CEL completions available. Use arrow keys and Enter to choose.";
      setActive(0);
    }

    function update(manual) {
      var context = completionContext(textarea.value, textarea.selectionStart, manual);
      if (!context || !context.items.length) {
        close();
        return;
      }
      render(context);
    }

    function accept(index) {
      var item = matches[index];
      if (!item) return;
      var inserted = item.insertText || item.label;
      textarea.value = textarea.value.slice(0, replaceStart) + inserted + textarea.value.slice(replaceEnd);
      var caret = replaceStart + inserted.length + (item.cursorOffset || 0);
      textarea.setSelectionRange(caret, caret);
      textarea.dispatchEvent(new Event("input", { bubbles: true }));
      close();
      textarea.focus();
    }

    textarea.addEventListener("focus", function () {
      loadCatalog().then(function () {
        if (document.activeElement === textarea) update(false);
      });
    });
    textarea.addEventListener("input", function () {
      update(false);
    });
    textarea.addEventListener("click", function () {
      update(false);
    });
    textarea.addEventListener("keyup", function (event) {
      if (["ArrowLeft", "ArrowRight", "Home", "End"].indexOf(event.key) !== -1) update(false);
    });
    textarea.addEventListener("keydown", function (event) {
      if (event.ctrlKey && !event.altKey && !event.metaKey && event.code === "Space") {
        event.preventDefault();
        update(true);
        return;
      }
      if (listbox.hidden) return;
      if (event.key === "ArrowDown") {
        event.preventDefault();
        setActive(activeIndex + 1);
      } else if (event.key === "ArrowUp") {
        event.preventDefault();
        setActive(activeIndex - 1);
      } else if (event.key === "Enter" || event.key === "Tab") {
        event.preventDefault();
        accept(activeIndex);
      } else if (event.key === "Escape") {
        event.preventDefault();
        close();
      }
    });
    textarea.addEventListener("blur", function () {
      window.setTimeout(close, 100);
    });
    document.addEventListener("pointerdown", function (event) {
      if (!editor.contains(event.target)) close();
    });

    attachLiveValidation(editor, textarea);
    loadCatalog();
  }

  function attachLiveValidation(editor, textarea) {
    var highlight = editor.querySelector("[data-cel-highlight]");
    var fieldset = editor.closest("fieldset");
    var feedback = fieldset && fieldset.querySelector("[data-cel-validation-feedback]");
    if (!highlight || !feedback) return;

    var validationTimer;
    var validationRequest;
    var validationSequence = 0;

    function syncHighlightScroll() {
      highlight.scrollTop = textarea.scrollTop;
      highlight.scrollLeft = textarea.scrollLeft;
    }

    function clearFeedback() {
      feedback.replaceChildren();
    }

    function showChecking() {
      clearFeedback();
      var checking = document.createElement("div");
      checking.className = "filter-validation-checking";
      checking.setAttribute("role", "status");
      checking.textContent = "Checking filters…";
      feedback.appendChild(checking);
    }

    function showValid() {
      clearFeedback();
      var valid = document.createElement("div");
      valid.className = "text-success text-sm";
      valid.setAttribute("role", "status");
      valid.textContent = "filters OK";
      feedback.appendChild(valid);
    }

    function showDiagnostics(diagnostics) {
      clearFeedback();
      var alert = document.createElement("div");
      alert.className = "alert alert-error filter-validation-errors";
      alert.dataset.status = "error";
      alert.dataset.variant = "destructive";
      alert.setAttribute("role", "alert");
      var list = document.createElement("ul");
      diagnostics.forEach(function (diagnostic) {
        var item = document.createElement("li");
        item.textContent = "Line " + diagnostic.line + ", column " + diagnostic.column + ": " + diagnostic.message;
        list.appendChild(item);
      });
      alert.appendChild(list);
      feedback.appendChild(alert);
    }

    function showUnavailable() {
      clearFeedback();
      var alert = document.createElement("div");
      alert.className = "alert alert-warning filter-validation-errors";
      alert.dataset.status = "warning";
      alert.setAttribute("role", "status");
      alert.textContent = "Live filter validation is temporarily unavailable. Save will still validate the filters.";
      feedback.appendChild(alert);
    }

    function renderHighlight(text, diagnostics) {
      var ranges = diagnosticRanges(text, diagnostics);
      var cursor = 0;
      highlight.replaceChildren();
      ranges.forEach(function (range) {
        if (range.start > cursor) {
          highlight.appendChild(document.createTextNode(text.slice(cursor, range.start)));
        }
        var error = document.createElement("mark");
        error.className = "cel-filter-error";
        error.textContent = text.slice(range.start, range.end);
        highlight.appendChild(error);
        cursor = range.end;
      });
      if (cursor < text.length) {
        highlight.appendChild(document.createTextNode(text.slice(cursor)));
      }
      if (text === "" || text.endsWith("\n")) {
        highlight.appendChild(document.createTextNode("\u200b"));
      }
      syncHighlightScroll();
    }

    function queueValidation() {
      window.clearTimeout(validationTimer);
      if (validationRequest) validationRequest.abort();
      validationRequest = null;
      validationSequence++;
      renderHighlight(textarea.value, []);
      textarea.setAttribute("aria-invalid", "false");
      textarea.removeAttribute("aria-busy");

      if (textarea.value.trim() === "") {
        clearFeedback();
        return;
      }

      showChecking();
      textarea.setAttribute("aria-busy", "true");
      var sequence = validationSequence;
      validationTimer = window.setTimeout(function () {
        validate(sequence, textarea.value);
      }, 300);
    }

    function validate(sequence, value) {
      validationRequest = new AbortController();
      var body = new URLSearchParams();
      body.set("filters", value);
      fetch("/frag/validate-filters", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/x-www-form-urlencoded"
        },
        body: body.toString(),
        credentials: "same-origin",
        signal: validationRequest.signal
      })
        .then(function (response) {
          if (!response.ok) throw new Error("validation request failed");
          return response.json();
        })
        .then(function (result) {
          if (sequence !== validationSequence || textarea.value !== value) return;
          var diagnostics = Array.isArray(result.diagnostics) ? result.diagnostics : [];
          renderHighlight(value, diagnostics);
          textarea.setAttribute("aria-busy", "false");
          textarea.setAttribute("aria-invalid", result.valid ? "false" : "true");
          if (result.valid) showValid();
          else showDiagnostics(diagnostics);
        })
        .catch(function (error) {
          if (error.name === "AbortError" || sequence !== validationSequence) return;
          textarea.setAttribute("aria-busy", "false");
          textarea.setAttribute("aria-invalid", "false");
          renderHighlight(textarea.value, []);
          showUnavailable();
        });
    }

    textarea.addEventListener("input", queueValidation);
    textarea.addEventListener("scroll", syncHighlightScroll);
    renderHighlight(textarea.value, []);
    if (textarea.value.trim() !== "") queueValidation();
  }

  function diagnosticRanges(text, diagnostics) {
    var ranges = (Array.isArray(diagnostics) ? diagnostics : [])
      .map(function (diagnostic) {
        var start = Math.max(0, Math.min(text.length, Number(diagnostic.start)));
        var end = Math.max(start, Math.min(text.length, Number(diagnostic.end)));
        if (end === start && start < text.length) end++;
        return { start: start, end: end };
      })
      .filter(function (range) {
        return range.end > range.start;
      })
      .sort(function (left, right) {
        return left.start - right.start || left.end - right.end;
      });

    return ranges.reduce(function (merged, range) {
      var previous = merged[merged.length - 1];
      if (previous && range.start <= previous.end) {
        previous.end = Math.max(previous.end, range.end);
      } else {
        merged.push(range);
      }
      return merged;
    }, []);
  }

  function completionContext(value, cursor, manual) {
    var lineStart = value.lastIndexOf("\n", cursor - 1) + 1;
    var before = value.slice(lineStart, cursor);
    var identifierSuffix = value.slice(cursor).match(/^[A-Za-z0-9_.]*/)[0];
    var numericSuffix = value.slice(cursor).match(/^\d*/)[0];

    var pgnMatch = before.match(/(?:msg\.pgn\s*(?:==|!=|<=|>=|<|>)\s*|msg\.pgn\s+in\s+\[[^\]]*?)(\d*)$/i);
    if (pgnMatch) {
      var digits = pgnMatch[1];
      return {
        start: cursor - digits.length,
        end: cursor + numericSuffix.length,
        items: rankedItems(catalog.pgns.map(catalogCompletion("pgn")), digits, numericLabel)
      };
    }

    var tokenMatch = before.match(/[A-Za-z_][A-Za-z0-9_.]*$/);
    var token = tokenMatch ? tokenMatch[0] : "";
    var start = cursor - token.length;
    var payloadPrefix = "msg.payload.";
    if (token.toLowerCase().indexOf(payloadPrefix) === 0) {
      var payloadQuery = token.slice(payloadPrefix.length);
      return {
        start: start,
        end: cursor + identifierSuffix.length,
        items: rankedItems(catalog.payloadFields.map(catalogCompletion("payload")), payloadQuery, tailLabel)
      };
    }

    if (!token && !manual) return null;
    return {
      start: start,
      end: cursor + identifierSuffix.length,
      items: rankedItems(envelopeItems.concat(languageItems), token, genericLabel),
      limit: manual && !token ? envelopeItems.length + languageItems.length : maxResults
    };
  }

  function catalogCompletion(kind) {
    return function (item) {
      return completion(item.label, item.detail, kind);
    };
  }

  function numericLabel(item) {
    return item.label;
  }

  function tailLabel(item) {
    var parts = item.label.split(".");
    return parts[parts.length - 1];
  }

  function genericLabel(item) {
    var label = item.label.replace(/\(\)$/, "");
    var parts = label.split(".");
    return label + " " + parts[parts.length - 1];
  }

  function rankedItems(items, query, searchable) {
    var normalized = query.toLowerCase();
    return items
      .map(function (item, index) {
        var text = searchable(item).toLowerCase();
        var score = normalized === "" ? 0 : matchScore(text, normalized);
        return { item: item, score: score, index: index };
      })
      .filter(function (entry) {
        return entry.score < 100;
      })
      .sort(function (left, right) {
        return left.score - right.score || left.index - right.index;
      })
      .map(function (entry) {
        return entry.item;
      });
  }

  function matchScore(text, query) {
    if (text === query) return 0;
    if (text.indexOf(query) === 0) return 1;
    var words = text.split(/[.\s]/);
    if (words.some(function (word) { return word.indexOf(query) === 0; })) return 2;
    if (text.indexOf(query) !== -1) return 3;
    return 100;
  }

  var activeSourceDeviceRow = null;

  function sourceDeviceRowFromTarget(target) {
    return target && target.closest && target.closest("[data-source-device-detail-row]");
  }

  function reconcileSourceDeviceRows(responseText) {
    var currentBody = document.getElementById("source-device-rows");
    if (!currentBody) return;

    var response = new DOMParser().parseFromString(responseText, "text/html");
    var snapshot = response.querySelector("[data-source-device-row-snapshot]");
    if (!snapshot) return;

    var nextRows = Array.prototype.slice.call(
      snapshot.querySelectorAll("[data-source-device-detail-row]")
    );
    var currentRows = Array.prototype.slice.call(
      currentBody.querySelectorAll("[data-source-device-detail-row]")
    );
    var currentByAddress = Object.create(null);
    var nextAddresses = Object.create(null);

    currentRows.forEach(function (row) {
      currentByAddress[row.dataset.deviceAddress] = row;
    });

    if (nextRows.length) {
      var empty = currentBody.querySelector("[data-source-device-empty-row]");
      if (empty) empty.remove();
    }

    nextRows.forEach(function (nextRow, index) {
      var address = nextRow.dataset.deviceAddress;
      var currentRow = currentByAddress[address];
      nextAddresses[address] = true;

      if (currentRow) {
        // Keep the row and its click handler stable. Replacing only its cells
        // avoids losing a click between pointer-down and pointer-up.
        if (currentRow !== activeSourceDeviceRow) {
          var cells = Array.prototype.map.call(nextRow.children, function (cell) {
            return cell.cloneNode(true);
          });
          currentRow.replaceChildren.apply(currentRow, cells);
        }
        return;
      }

      var inserted = nextRow.cloneNode(true);
      var before = null;
      for (var nextIndex = index + 1; nextIndex < nextRows.length; nextIndex += 1) {
        before = currentByAddress[nextRows[nextIndex].dataset.deviceAddress];
        if (before) break;
      }
      currentBody.insertBefore(inserted, before);
      currentByAddress[address] = inserted;
      if (window.htmx && typeof window.htmx.process === "function") {
        window.htmx.process(inserted);
      }
    });

    currentRows.forEach(function (row) {
      if (!nextAddresses[row.dataset.deviceAddress] && row !== activeSourceDeviceRow) {
        row.remove();
      }
    });

    if (!nextRows.length && !currentBody.querySelector("[data-source-device-empty-row]")) {
      var nextEmpty = snapshot.querySelector("[data-source-device-empty-row]");
      if (nextEmpty) currentBody.appendChild(nextEmpty.cloneNode(true));
    }
  }

  function syncSourceDevicePGNSortControls(responseText) {
    var response = new DOMParser().parseFromString(responseText, "text/html");
    var nextControls = response.querySelectorAll("[data-source-device-pgn-sort-control]");

    Array.prototype.forEach.call(nextControls, function (nextControl) {
      var key = nextControl.dataset.sourceDevicePgnSortControl;
      var currentControl = document.querySelector(
        '[data-source-device-pgn-sort-control="' + key + '"]'
      );
      if (!currentControl) return;

      var currentHeading = currentControl.closest("th");
      var nextHeading = nextControl.closest("th");
      if (currentHeading && nextHeading) {
        currentHeading.setAttribute("aria-sort", nextHeading.getAttribute("aria-sort") || "none");
      }

      var replacement = nextControl.cloneNode(true);
      currentControl.replaceWith(replacement);
      if (window.htmx && typeof window.htmx.process === "function") {
        window.htmx.process(replacement);
      }
    });
  }

  var streamCaptureLimit = 200;

  function initStreamPanels(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var panels = Array.prototype.slice.call(scope.querySelectorAll("[data-stream-panel]"));
    if (scope.matches && scope.matches("[data-stream-panel]")) panels.unshift(scope);
    panels.forEach(initStreamPanel);
  }

  function initStreamPanel(panel) {
    if (panel.dataset.streamReady === "true") return;
    panel.dataset.streamReady = "true";

    var startButton = panel.querySelector("[data-stream-start]");
    var stopButton = panel.querySelector("[data-stream-stop]");
    var clearButton = panel.querySelector("[data-stream-clear]");
    var copyButton = panel.querySelector("[data-stream-copy]");
    var copyFeedback = panel.querySelector("[data-stream-copy-feedback]");
    var filterInput = panel.querySelector("[data-stream-filter]");
    var filterFeedback = panel.querySelector("[data-stream-filter-feedback]");
    var celInspector = panel.querySelector("[data-stream-cel-inspector]");
    var celPath = panel.querySelector("[data-stream-cel-path]");
    var celExpression = panel.querySelector("[data-stream-cel-expression]");
    var celUseButton = panel.querySelector("[data-stream-cel-use]");
    var celCopyButton = panel.querySelector("[data-stream-cel-copy]");
    var celCloseButton = panel.querySelector("[data-stream-cel-close]");
    var status = panel.querySelector("[data-stream-status]");
    var empty = panel.querySelector("[data-stream-empty]");
    var emptyTitle = empty && empty.querySelector("strong");
    var emptyDetail = empty && empty.querySelector("span");
    var list = panel.querySelector("[data-stream-list]");
    var viewButtons = Array.prototype.slice.call(panel.querySelectorAll("[data-stream-view]"));
    var exportButtons = Array.prototype.slice.call(panel.querySelectorAll("[data-stream-export]"));
    if (!startButton || !stopButton || !clearButton || !status || !empty || !list) return;

    var entries = [];
    var totalCaptured = 0;
    var stream = null;
    var starting = false;
    var appliedFilter = "";
    var displayFormat = "json";
    var renderPending = false;
    var filterTimer = null;
    var validationSequence = 0;
    var copyFeedbackTimer = null;

    function isStreaming() {
      return stream !== null;
    }

    function setStatus(label) {
      var summary = totalCaptured.toLocaleString() + " captured";
      if (totalCaptured > entries.length) {
        summary += " \u00b7 latest " + entries.length.toLocaleString() + " shown";
      }
      status.textContent = label + " \u00b7 " + summary;
    }

    function setFilterError(message) {
      if (!filterInput || !filterFeedback) return;
      filterInput.setAttribute("aria-invalid", message ? "true" : "false");
      filterFeedback.textContent = message || "";
      filterFeedback.hidden = !message;
    }

    function updateControls() {
      startButton.hidden = isStreaming();
      stopButton.hidden = !isStreaming();
      startButton.disabled = starting;
      stopButton.disabled = false;
      if (filterInput) filterInput.disabled = starting;
      clearButton.disabled = entries.length === 0;
      exportButtons.forEach(function (button) {
        if (button.dataset.streamExport === "can") {
          button.disabled = !entries.some(function (entry) {
            return streamRawBytes(entry.raw).length > 0;
          });
        } else {
          button.disabled = entries.length === 0;
        }
      });
      if (copyButton) {
        copyButton.disabled = entries.length === 0 || (
          displayFormat === "can" && !entries.some(function (entry) {
            return streamRawBytes(entry.raw).length > 0;
          })
        );
      }
    }

    function updateEmpty() {
      if (entries.length > 0) {
        empty.hidden = true;
        list.hidden = false;
        return;
      }
      empty.hidden = false;
      list.hidden = true;
      if (isStreaming()) {
        emptyTitle.textContent = "Waiting for messages\u2026";
        emptyDetail.textContent = "New " + panel.dataset.streamKind + " messages will appear here.";
      } else {
        emptyTitle.textContent = "Streaming is stopped.";
        emptyDetail.textContent = "Choose Start to capture messages arriving at this " + panel.dataset.streamKind + ".";
      }
    }

    function queueRender() {
      if (renderPending) return;
      renderPending = true;
      window.requestAnimationFrame(function () {
        renderPending = false;
        renderEntries();
      });
    }

    function renderEntries() {
      var fragment = document.createDocumentFragment();
      entries.forEach(function (entry) {
        fragment.appendChild(streamMessageElement(entry, displayFormat));
      });
      list.replaceChildren(fragment);
      updateEmpty();
      updateControls();
      setStatus(isStreaming() ? "Streaming" : "Stopped");
    }

    function validateFilter(filterExpression) {
      if (!filterExpression) {
        return Promise.resolve({ valid: true, diagnostics: [] });
      }
      var body = new URLSearchParams();
      body.set("filters", filterExpression);
      return fetch("/frag/validate-filters", {
        method: "POST",
        headers: {
          Accept: "application/json",
          "Content-Type": "application/x-www-form-urlencoded"
        },
        body: body.toString(),
        credentials: "same-origin"
      }).then(function (response) {
        if (!response.ok) throw new Error("validation request failed");
        return response.json();
      });
    }

    function validationError(result, suffix) {
      var diagnostics = Array.isArray(result.diagnostics) ? result.diagnostics : [];
      var message = diagnostics.length > 0
        ? "Invalid CEL: " + diagnostics[0].message
        : "Invalid CEL filter.";
      return suffix ? message + " " + suffix : message;
    }

    function openStream(filterExpression) {
      if (stream) stream.close();
      var streamURL = new URL(panel.dataset.streamUrl, window.location.href);
      if (filterExpression) streamURL.searchParams.set("filter", filterExpression);
      var nextStream = new EventSource(streamURL.toString());
      stream = nextStream;
      appliedFilter = filterExpression;
      starting = false;
      setFilterError("");
      setStatus("Connecting");
      updateControls();
      updateEmpty();

      nextStream.onopen = function () {
        if (stream === nextStream) setStatus("Streaming");
      };
      nextStream.onmessage = function (event) {
        if (stream !== nextStream) return;
        var envelope;
        try {
          envelope = JSON.parse(event.data);
        } catch (_) {
          return;
        }
        if (!envelope || typeof envelope !== "object" || !("payload" in envelope)) return;
        // Keep the exact Go-produced payload text alongside the parsed object.
        // JSON.parse would round uint64/int64 values above JavaScript's safe
        // integer range, corrupting both the inspector and a later export.
        envelope.streamPayloadJSON = streamPayloadJSON(event.data) || JSON.stringify(envelope.payload);
        totalCaptured += 1;
        entries.unshift(envelope);
        if (entries.length > streamCaptureLimit) entries.length = streamCaptureLimit;
        queueRender();
      };
      nextStream.onerror = function () {
        if (stream === nextStream) setStatus("Reconnecting");
      };
    }

    function start() {
      if (stream || starting || !panel.dataset.streamUrl) return;
      var filterExpression = filterInput ? filterInput.value.trim() : "";
      setFilterError("");
      if (!filterExpression) {
        openStream("");
        return;
      }

      starting = true;
      var sequence = ++validationSequence;
      setStatus("Checking filter");
      updateControls();
      validateFilter(filterExpression)
        .then(function (result) {
          if (!starting || sequence !== validationSequence) return;
          if (!result.valid) {
            starting = false;
            setFilterError(validationError(result));
            setStatus("Filter error");
            updateControls();
            return;
          }
          openStream(filterExpression);
        })
        .catch(function () {
          if (!starting || sequence !== validationSequence) return;
          starting = false;
          setFilterError("CEL validation is unavailable. Try again.");
          setStatus("Filter error");
          updateControls();
        });
    }

    function applyRunningFilter() {
      window.clearTimeout(filterTimer);
      filterTimer = null;
      if (!stream || !filterInput) return;
      var filterExpression = filterInput.value.trim();
      if (filterExpression === appliedFilter) {
        setFilterError("");
        return;
      }

      var sequence = ++validationSequence;
      validateFilter(filterExpression)
        .then(function (result) {
          if (!stream || sequence !== validationSequence) return;
          if (!result.valid) {
            setFilterError(validationError(result, "The current stream filter is unchanged."));
            return;
          }
          openStream(filterExpression);
        })
        .catch(function () {
          if (!stream || sequence !== validationSequence) return;
          setFilterError("CEL validation is unavailable. The current stream filter is unchanged.");
        });
    }

    function scheduleRunningFilter() {
      window.clearTimeout(filterTimer);
      filterTimer = window.setTimeout(applyRunningFilter, 350);
    }

    function stop() {
      window.clearTimeout(filterTimer);
      filterTimer = null;
      validationSequence += 1;
      if (stream) stream.close();
      stream = null;
      starting = false;
      updateControls();
      updateEmpty();
      setStatus("Stopped");
    }

    function clear() {
      entries = [];
      totalCaptured = 0;
      list.replaceChildren();
      if (celInspector) celInspector.hidden = true;
      if (copyFeedback) copyFeedback.textContent = "";
      updateControls();
      updateEmpty();
      setStatus(isStreaming() ? "Streaming" : "Stopped");
    }

    startButton.addEventListener("click", start);
    stopButton.addEventListener("click", stop);
    clearButton.addEventListener("click", clear);
    if (filterInput) {
      filterInput.addEventListener("input", function () {
        setFilterError("");
        if (isStreaming()) scheduleRunningFilter();
      });
      filterInput.addEventListener("keydown", function (event) {
        if (event.key !== "Enter") return;
        event.preventDefault();
        if (isStreaming()) applyRunningFilter();
        else if (!startButton.disabled) start();
      });
    }

    list.addEventListener("click", function (event) {
      var content = event.target.closest && event.target.closest(".stream-message-content[data-stream-json]");
      if (!content || !celInspector || !celPath || !celExpression) return;
      var target = event.target.closest("[data-cel-expression]");
      celPath.textContent = target ? target.dataset.celPath : "msg.payload";
      celExpression.textContent = target ? target.dataset.celExpression : "has(msg.payload)";
      celInspector.hidden = false;
    });

    if (celUseButton && celExpression && filterInput) {
      celUseButton.addEventListener("click", function () {
        filterInput.value = celExpression.textContent;
        setFilterError("");
        filterInput.focus();
        if (isStreaming()) applyRunningFilter();
      });
    }

    if (celCopyButton && celExpression) {
      celCopyButton.addEventListener("click", function () {
        writeClipboard(celExpression.textContent).then(function () {
          if (copyFeedback) copyFeedback.textContent = "CEL copied.";
        }).catch(function () {
          if (copyFeedback) copyFeedback.textContent = "Copy failed.";
        });
      });
    }

    if (celCloseButton && celInspector) {
      celCloseButton.addEventListener("click", function () {
        celInspector.hidden = true;
      });
    }

    viewButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        displayFormat = button.dataset.streamView;
        viewButtons.forEach(function (candidate) {
          var selected = candidate === button;
          candidate.setAttribute("aria-pressed", selected ? "true" : "false");
          candidate.classList.toggle("btn-primary", selected);
          candidate.classList.toggle("btn-ghost", !selected);
          candidate.dataset.variant = selected ? "primary" : "ghost";
        });
        if (displayFormat !== "json" && celInspector) celInspector.hidden = true;
        queueRender();
      });
    });

    exportButtons.forEach(function (button) {
      button.addEventListener("click", function () {
        exportStream(entries, button.dataset.streamExport, panel.dataset.streamKind, panel.dataset.streamId);
      });
    });

    if (copyButton) {
      copyButton.addEventListener("click", function () {
        var output = streamClipboardText(entries, displayFormat);
        if (!output) return;
        writeClipboard(output).then(function () {
          window.clearTimeout(copyFeedbackTimer);
          if (copyFeedback) {
            var copiedLines = output.trimEnd().split("\n").length;
            copyFeedback.textContent = "Copied " + copiedLines.toLocaleString() + " retained message" +
              (copiedLines === 1 ? "." : "s.");
          }
          copyFeedbackTimer = window.setTimeout(function () {
            if (copyFeedback) copyFeedback.textContent = "";
          }, 2500);
        }).catch(function () {
          if (copyFeedback) copyFeedback.textContent = "Copy failed.";
        });
      });
    }

    panel.stopStreamCapture = stop;
    updateControls();
    updateEmpty();
  }

  function streamMessageElement(envelope, format) {
    var article = document.createElement("article");
    article.className = "stream-message";
    var payload = envelope.payload && typeof envelope.payload === "object" ? envelope.payload : {};
    var info = payload.info && typeof payload.info === "object" ? payload.info : {};
    var metadata = envelope.metadata && typeof envelope.metadata === "object" ? envelope.metadata : {};
    var identityParts = [];
    if (info.pgn !== undefined && info.pgn !== null) identityParts.push("PGN " + info.pgn);
    if (metadata.pgn_name) identityParts.push(metadata.pgn_name);
    if (info.sourceId !== undefined && info.sourceId !== null) identityParts.push("source " + info.sourceId);
    var bytes = streamRawBytes(envelope.raw);
    article.title = identityParts.join(" \u00b7 ") || "N2K message";
    var code = document.createElement("code");
    code.className = "stream-message-content";
    if (format === "can") {
      code.textContent = bytes.length ? streamBytesHex(bytes, true) : "No raw CAN payload available.";
    } else {
      code.dataset.streamJson = "true";
      code.title = "Click a JSON key or value to inspect its CEL filter";
      renderStreamJSON(code, envelope.streamPayloadJSON || JSON.stringify(envelope.payload));
    }
    article.appendChild(code);
    return article;
  }

  function renderStreamJSON(code, text) {
    var targets = streamJSONCELTargets(text);
    if (targets.length === 0) {
      code.textContent = text;
      return;
    }
    var cursor = 0;
    targets.forEach(function (target) {
      if (target.start < cursor) return;
      if (target.start > cursor) {
        code.appendChild(document.createTextNode(text.slice(cursor, target.start)));
      }
      var span = document.createElement("span");
      span.className = "stream-json-cel-target";
      span.dataset.celPath = target.path;
      span.dataset.celExpression = target.expression;
      span.textContent = text.slice(target.start, target.end);
      span.title = target.expression;
      code.appendChild(span);
      cursor = target.end;
    });
    if (cursor < text.length) {
      code.appendChild(document.createTextNode(text.slice(cursor)));
    }
  }

  function streamJSONCELTargets(text) {
    var targets = [];
    var cursor = 0;

    function skipWhitespace() {
      while (cursor < text.length && /\s/.test(text[cursor])) cursor += 1;
    }

    function readString() {
      var start = cursor;
      cursor += 1;
      var escaped = false;
      while (cursor < text.length) {
        var character = text[cursor];
        cursor += 1;
        if (escaped) {
          escaped = false;
        } else if (character === "\\") {
          escaped = true;
        } else if (character === '"') {
          break;
        }
      }
      var raw = text.slice(start, cursor);
      return { start: start, end: cursor, raw: raw, value: JSON.parse(raw) };
    }

    function presenceExpression(path) {
      return path.endsWith("]") ? path + " != null" : "has(" + path + ")";
    }

    function childPath(path, key) {
      return /^[A-Za-z_][A-Za-z0-9_]*$/.test(key)
        ? path + "." + key
        : path + "[" + JSON.stringify(key) + "]";
    }

    function primitiveExpression(path, raw) {
      if (/^-?\d+$/.test(raw) && typeof BigInt === "function") {
        try {
          var integer = BigInt(raw);
          if (integer > BigInt("9223372036854775807") || integer < BigInt("-9223372036854775808")) {
            return presenceExpression(path);
          }
        } catch (_) {
          return presenceExpression(path);
        }
      }
      return path + " == " + raw;
    }

    function parseValue(path) {
      skipWhitespace();
      var start = cursor;
      var character = text[cursor];
      if (character === "{") {
        cursor += 1;
        skipWhitespace();
        while (cursor < text.length && text[cursor] !== "}") {
          var key = readString();
          skipWhitespace();
          if (text[cursor] !== ":") throw new Error("invalid JSON object");
          cursor += 1;
          var nextPath = childPath(path, key.value);
          var value = parseValue(nextPath);
          targets.push({
            start: key.start,
            end: key.end,
            path: nextPath,
            expression: value.expression
          });
          skipWhitespace();
          if (text[cursor] === ",") {
            cursor += 1;
            skipWhitespace();
          } else {
            break;
          }
        }
        if (text[cursor] !== "}") throw new Error("invalid JSON object");
        cursor += 1;
        return { start: start, end: cursor, expression: presenceExpression(path) };
      }
      if (character === "[") {
        cursor += 1;
        skipWhitespace();
        var arrayIndex = 0;
        while (cursor < text.length && text[cursor] !== "]") {
          parseValue(path + "[" + arrayIndex + "]");
          arrayIndex += 1;
          skipWhitespace();
          if (text[cursor] === ",") {
            cursor += 1;
            skipWhitespace();
          } else {
            break;
          }
        }
        if (text[cursor] !== "]") throw new Error("invalid JSON array");
        cursor += 1;
        return { start: start, end: cursor, expression: presenceExpression(path) };
      }
      if (character === '"') {
        var stringValue = readString();
        var stringExpression = path + " == " + stringValue.raw;
        targets.push({
          start: stringValue.start,
          end: stringValue.end,
          path: path,
          expression: stringExpression
        });
        return { start: start, end: cursor, expression: stringExpression };
      }

      while (cursor < text.length && !/[\s,\]}]/.test(text[cursor])) cursor += 1;
      if (cursor === start) throw new Error("invalid JSON value");
      var raw = text.slice(start, cursor);
      var expression = primitiveExpression(path, raw);
      targets.push({ start: start, end: cursor, path: path, expression: expression });
      return { start: start, end: cursor, expression: expression };
    }

    try {
      parseValue("msg.payload");
      skipWhitespace();
      if (cursor !== text.length) return [];
    } catch (_) {
      return [];
    }
    return targets.sort(function (left, right) {
      return left.start - right.start || left.end - right.end;
    });
  }

  function streamRawBytes(raw) {
    if (typeof raw !== "string" || raw === "") return new Uint8Array(0);
    try {
      var decoded = window.atob(raw);
      var bytes = new Uint8Array(decoded.length);
      for (var index = 0; index < decoded.length; index += 1) {
        bytes[index] = decoded.charCodeAt(index);
      }
      return bytes;
    } catch (_) {
      return new Uint8Array(0);
    }
  }

  function streamBytesHex(bytes, spaced) {
    return Array.prototype.map.call(bytes, function (byte) {
      return byte.toString(16).padStart(2, "0").toUpperCase();
    }).join(spaced ? " " : "");
  }

  function streamPayloadJSON(documentText) {
    // MarshalJSON emits payload as the first top-level field. Extract that
    // value without parsing it so integer tokens and native n2k key order stay
    // exactly as Go serialized them.
    var keyIndex = documentText.indexOf('"payload"');
    if (keyIndex < 0) return "";
    var colon = documentText.indexOf(":", keyIndex + 9);
    if (colon < 0) return "";
    var start = colon + 1;
    while (start < documentText.length && /\s/.test(documentText[start])) start += 1;
    if (start >= documentText.length) return "";

    var opening = documentText[start];
    if (opening !== "{" && opening !== "[" && opening !== '"') {
      var primitiveEnd = start;
      while (primitiveEnd < documentText.length && documentText[primitiveEnd] !== "," && documentText[primitiveEnd] !== "}") {
        primitiveEnd += 1;
      }
      return documentText.slice(start, primitiveEnd).trim();
    }

    var depth = 0;
    var inString = false;
    var escaped = false;
    for (var index = start; index < documentText.length; index += 1) {
      var character = documentText[index];
      if (inString) {
        if (escaped) escaped = false;
        else if (character === "\\") escaped = true;
        else if (character === '"') {
          inString = false;
          if (opening === '"' && depth === 0) return documentText.slice(start, index + 1);
        }
        continue;
      }
      if (character === '"') {
        inString = true;
      } else if (character === "{" || character === "[") {
        depth += 1;
      } else if (character === "}" || character === "]") {
        depth -= 1;
        if (depth === 0) return documentText.slice(start, index + 1);
      }
    }
    return "";
  }

  function streamClipboardText(entries, format) {
    var chronological = entries.slice().reverse();
    var lines;
    if (format === "can") {
      lines = chronological
        .map(function (entry) { return streamRawBytes(entry.raw); })
        .filter(function (bytes) { return bytes.length > 0; })
        .map(function (bytes) { return streamBytesHex(bytes, true); });
    } else {
      lines = chronological.map(function (entry) {
        return entry.streamPayloadJSON || JSON.stringify(entry.payload);
      });
    }
    return lines.length ? lines.join("\n") + "\n" : "";
  }

  function writeClipboard(text) {
    if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
      return navigator.clipboard.writeText(text);
    }
    return new Promise(function (resolve, reject) {
      var textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "");
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.appendChild(textarea);
      textarea.select();
      var copied = document.execCommand("copy");
      textarea.remove();
      if (copied) resolve();
      else reject(new Error("copy failed"));
    });
  }

  function exportStream(entries, format, kind, id) {
    var chronological = entries.slice().reverse();
    var body;
    var extension;
    var mime;
    if (format === "can") {
      body = chronological
        .map(function (entry) { return streamRawBytes(entry.raw); })
        .filter(function (bytes) { return bytes.length > 0; })
        .map(function (bytes) { return streamBytesHex(bytes, false); })
        .join("\n") + "\n";
      extension = "can.hex";
      mime = "text/plain";
    } else {
      body = chronological.map(function (entry) {
        return entry.streamPayloadJSON || JSON.stringify(entry.payload);
      }).join("\n") + "\n";
      extension = "n2k.jsonl";
      mime = "application/x-ndjson";
    }

    var safeID = String(id || "stream").replace(/[^A-Za-z0-9_.-]+/g, "-");
    var stamp = new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
    var name = String(kind || "component") + "-" + safeID + "-" + stamp + "." + extension;
    var url = URL.createObjectURL(new Blob([body], { type: mime }));
    var link = document.createElement("a");
    link.href = url;
    link.download = name;
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.setTimeout(function () { URL.revokeObjectURL(url); }, 0);
  }

  var entityDialogReturnFocus = null;

  function initEntityCreateDialogs(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var dialogs = Array.prototype.slice.call(scope.querySelectorAll("[data-entity-create-dialog]"));
    if (scope.matches && scope.matches("[data-entity-create-dialog]")) dialogs.unshift(scope);
    dialogs.forEach(function (dialog) {
      if (dialog.dataset.dialogReady === "true") return;
      dialog.dataset.dialogReady = "true";

      function requestClose(event) {
        if (event) event.preventDefault();
        var container = dialog.closest("#entity-create-dialog-container");
        if (dialog.open && typeof dialog.close === "function") dialog.close();
        if (container) container.replaceChildren();
        else dialog.remove();
        if (entityDialogReturnFocus && document.contains(entityDialogReturnFocus)) {
          entityDialogReturnFocus.focus();
        }
        entityDialogReturnFocus = null;
      }

      dialog.querySelectorAll("[data-entity-create-dialog-close]").forEach(function (button) {
        button.addEventListener("click", requestClose);
      });
      dialog.addEventListener("cancel", requestClose);
      dialog.addEventListener("click", function (event) {
        if (event.target === dialog) requestClose(event);
      });

      if (typeof dialog.showModal === "function") {
        if (!dialog.open) dialog.showModal();
      } else {
        dialog.setAttribute("open", "");
      }
    });
  }

  function initSourceDeviceDialog(root) {
    var scope = root && root.querySelector ? root : document;
    var dialog = scope.matches && scope.matches("[data-source-device-dialog]")
      ? scope
      : scope.querySelector("[data-source-device-dialog]");
    if (!dialog || dialog.dataset.dialogReady === "true") return;
    dialog.dataset.dialogReady = "true";

    var close = dialog.querySelector("[data-source-device-dialog-close]");
    function requestClose(event) {
      if (event) event.preventDefault();
      if (close) close.click();
      else dialog.close();
    }

    dialog.addEventListener("cancel", requestClose);
    dialog.addEventListener("click", function (event) {
      if (event.target === dialog) requestClose(event);
    });

    if (typeof dialog.showModal === "function") {
      if (!dialog.open) dialog.showModal();
    } else {
      dialog.setAttribute("open", "");
    }
  }

  function initDAGs(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var boards = Array.prototype.slice.call(scope.querySelectorAll("[data-dag-board]"));
    if (scope.matches && scope.matches("[data-dag-board]")) boards.unshift(scope);
    boards.forEach(initDAG);
  }

  function initDAG(board) {
    if (board.dataset.dagReady === "true") return;
    var svg = board.querySelector(".dag-edges");
    if (!svg) return;
    board.dataset.dagReady = "true";
    svg.style.setProperty("--dag-edge-flash-delay", (-window.performance.now()) + "ms");

    var frame = 0;

    function scheduleDraw() {
      if (frame) window.cancelAnimationFrame(frame);
      frame = window.requestAnimationFrame(draw);
    }

    function draw() {
      frame = 0;
      if (!board.isConnected) return;

      var boardRect = board.getBoundingClientRect();
      var nodes = new Map();
      board.querySelectorAll("[data-dag-node]").forEach(function (node) {
        var rect = node.getBoundingClientRect();
        nodes.set(node.dataset.dagNode, {
          element: node,
          left: rect.left - boardRect.left + board.scrollLeft,
          right: rect.right - boardRect.left + board.scrollLeft,
          top: rect.top - boardRect.top + board.scrollTop,
          height: rect.height,
          centerY: rect.top - boardRect.top + board.scrollTop + rect.height / 2
        });
      });

      var width = Math.max(board.clientWidth, board.scrollWidth);
      var height = Math.max(board.clientHeight, board.scrollHeight);
      svg.setAttribute("width", String(width));
      svg.setAttribute("height", String(height));
      svg.setAttribute("viewBox", "0 0 " + width + " " + height);
      svg.style.width = width + "px";
      svg.style.height = height + "px";

      var edges = [];
      svg.querySelectorAll("[data-dag-from][data-dag-to]").forEach(function (path) {
        var from = nodes.get(path.dataset.dagFrom);
        var to = nodes.get(path.dataset.dagTo);
        if (!from || !to) {
          path.removeAttribute("d");
          return;
        }
        edges.push({ path: path, from: from, to: to });
      });

      var outgoing = new Map();
      var incoming = new Map();
      edges.forEach(function (edge) {
        if (!outgoing.has(edge.from.element)) outgoing.set(edge.from.element, []);
        if (!incoming.has(edge.to.element)) incoming.set(edge.to.element, []);
        outgoing.get(edge.from.element).push(edge);
        incoming.get(edge.to.element).push(edge);
      });

      outgoing.forEach(function (nodeEdges) {
        nodeEdges.sort(function (left, right) {
          return left.to.centerY - right.to.centerY;
        });
        distributeDAGPorts(nodeEdges, "startY", "from");
      });
      incoming.forEach(function (nodeEdges) {
        nodeEdges.sort(function (left, right) {
          return left.from.centerY - right.from.centerY;
        });
        distributeDAGPorts(nodeEdges, "endY", "to");
      });

      edges.forEach(function (edge) {
        var startX = edge.from.right + 1;
        var endX = edge.to.left - 3;
        var control = Math.max(18, (endX - startX) * 0.45);
        edge.path.setAttribute(
          "d",
          "M " + startX + " " + edge.startY +
          " C " + (startX + control) + " " + edge.startY +
          ", " + (endX - control) + " " + edge.endY +
          ", " + endX + " " + edge.endY
        );
      });
    }

    var resizeObserver;
    if (typeof ResizeObserver === "function") {
      resizeObserver = new ResizeObserver(scheduleDraw);
      resizeObserver.observe(board);
    } else {
      window.addEventListener("resize", scheduleDraw);
    }

    board.cleanupDAG = function () {
      if (frame) window.cancelAnimationFrame(frame);
      if (resizeObserver) resizeObserver.disconnect();
      else window.removeEventListener("resize", scheduleDraw);
    };
    // Draw before the browser paints the freshly swapped htmx fragment. Waiting
    // for the next animation frame makes every two-second poll briefly show an
    // empty SVG, which reads as an edge flash.
    draw();
  }

  function distributeDAGPorts(edges, property, nodeProperty) {
    var node = edges[0] && edges[0][nodeProperty];
    if (!node) return;
    var padding = Math.min(24, node.height * 0.22);
    var usable = Math.max(0, node.height - padding * 2);
    edges.forEach(function (edge, index) {
      var ratio = edges.length === 1 ? 0.5 : index / (edges.length - 1);
      edge[property] = node.top + padding + usable * ratio;
    });
  }

  function cleanupDAGs(root) {
    if (!root || !root.querySelectorAll) return;
    var boards = Array.prototype.slice.call(root.querySelectorAll("[data-dag-board]"));
    if (root.matches && root.matches("[data-dag-board]")) boards.unshift(root);
    boards.forEach(function (board) {
      if (typeof board.cleanupDAG === "function") board.cleanupDAG();
    });
  }

  document.addEventListener("pointerdown", function (event) {
    activeSourceDeviceRow = sourceDeviceRowFromTarget(event.target);
  });
  ["pointerup", "pointercancel"].forEach(function (eventName) {
    document.addEventListener(eventName, function () {
      window.setTimeout(function () {
        activeSourceDeviceRow = null;
      }, 0);
    });
  });

  document.addEventListener("keydown", function (event) {
    var row = sourceDeviceRowFromTarget(event.target);
    if (row && (event.key === "Enter" || event.key === " " || event.key === "Spacebar")) {
      event.preventDefault();
      row.click();
      return;
    }
    if (event.key !== "Escape") return;
    var panel = document.getElementById("source-device-detail-panel");
    var close = panel && panel.tagName === "DIALOG" && panel.open &&
      panel.querySelector("[data-source-device-dialog-close]");
    if (close) {
      event.preventDefault();
      close.click();
    }
  });

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      initCELAutocomplete(document);
      initEntityCreateDialogs(document);
      initSourceDeviceDialog(document);
      initStreamPanels(document);
      initDAGs(document);
    });
  } else {
    initCELAutocomplete(document);
    initEntityCreateDialogs(document);
    initSourceDeviceDialog(document);
    initStreamPanels(document);
    initDAGs(document);
  }
  document.addEventListener("htmx:beforeRequest", function (event) {
    var trigger = event.detail && event.detail.elt;
    if (trigger && trigger.getAttribute("hx-target") === "#entity-create-dialog-container") {
      entityDialogReturnFocus = trigger;
    }
  });
  document.addEventListener("htmx:afterSwap", function (event) {
    var root = event.detail && event.detail.target ? event.detail.target : event.target;
    initDAGs(root);
  });
  document.addEventListener("htmx:load", function (event) {
    var root = event.detail && event.detail.elt ? event.detail.elt : document;
    initCELAutocomplete(root);
    initEntityCreateDialogs(root);
    initSourceDeviceDialog(root);
    initStreamPanels(root);
    initDAGs(root);
  });
  document.addEventListener("htmx:beforeCleanupElement", function (event) {
    var root = event.detail && event.detail.elt;
    if (!root || !root.querySelectorAll) return;
    cleanupDAGs(root);
    var panels = Array.prototype.slice.call(root.querySelectorAll("[data-stream-panel]"));
    if (root.matches && root.matches("[data-stream-panel]")) panels.unshift(root);
    panels.forEach(function (panel) {
      if (typeof panel.stopStreamCapture === "function") panel.stopStreamCapture();
    });
  });
  document.addEventListener("htmx:afterRequest", function (event) {
    var trigger = event.detail && event.detail.elt;
    if (!trigger) return;
    if (!event.detail.successful) return;
    if (trigger.matches("[data-source-device-row-refresh]")) {
      reconcileSourceDeviceRows(event.detail.xhr.responseText);
      return;
    }
    if (trigger.matches("[data-source-device-pgn-sort-control]")) {
      syncSourceDevicePGNSortControls(event.detail.xhr.responseText);
    }
  });
})();
