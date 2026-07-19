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
    catalogPromise = fetch("/ui/cel-completions", {
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

    loadCatalog();
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

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      initCELAutocomplete(document);
    });
  } else {
    initCELAutocomplete(document);
  }
  document.addEventListener("htmx:load", function (event) {
    initCELAutocomplete(event.detail && event.detail.elt ? event.detail.elt : document);
  });
})();
