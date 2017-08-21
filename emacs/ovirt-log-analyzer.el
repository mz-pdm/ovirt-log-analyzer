;;; Major mode for inspecting oVirt log analyzer result output

(require 'hl-line)

(defun ovirt-log-analyzer-show-log ()
  (interactive)
  (let (file
        line)
    (save-excursion
      (beginning-of-line)
      (unless (looking-at "[^|\n]*| *\\([^|\n]+\\):\\([0-9]+\\) *|")
        (error "No file reference found"))
      (setq file (match-string 1)
            line (string-to-number (match-string 2))))
    (find-file file)
    (goto-char 1)
    (forward-line (1- line))))

(defun ovirt-log-analyzer-poi (search-function)
  (let ((point (funcall search-function (point) 'face)))
    (while (and point
                (not (eq (get-text-property point 'face) 'font-lock-variable-name-face)))
      (setq point (funcall search-function point 'face)))
    (if point
        (goto-char point)
      (message "No further point of interest"))))

(defun ovirt-log-analyzer-next-poi ()
  (interactive)
  (ovirt-log-analyzer-poi 'next-single-property-change))

(defun ovirt-log-analyzer-previous-poi ()
  (interactive)
  (ovirt-log-analyzer-poi 'previous-single-property-change))

(defun ovirt-log-analyzer-filter ()
  (interactive)
  (unless (eq (get-text-property (point) 'face) 'font-lock-variable-name-face)
    (error "Not on a UUID"))
  (dolist (o (overlays-in (point-min) (point-max)))
    (when (overlay-get o 'ovirt-log-analyzer-filter)
      (delete-overlay o)))
  (let ((uuid-regexp "[0-9a-f]\\{8\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{12\\}"))
    (save-excursion
      (unless (looking-at uuid-regexp)
        (goto-char (previous-single-property-change (point) 'face))
        (looking-at uuid-regexp))
      (let ((uuid (match-string 0)))
        (goto-char (point-min))
        (while (not (eobp))
          (unless (search-forward uuid (line-end-position) t)
            (let ((o (make-overlay (line-beginning-position) (1+ (line-end-position)))))
              (overlay-put o 'invisible t)
              (overlay-put o 'ovirt-log-analyzer-filter t)))
          (forward-line))))))

(defun ovirt-log-analyzer-toggle-filter ()
  (interactive)
  (dolist (o (overlays-in (point-min) (point-max)))
    (when (overlay-get o 'ovirt-log-analyzer-filter)
      (overlay-put o 'invisible (not (overlay-get o 'invisible))))))

(defun ovirt-log-analyzer-process ()
  (save-excursion
    (goto-char (point-min))
    (remove-overlays)
    (let ((unknown-tags '()))
      (while (not (eobp))
        (when (looking-at "[0-9][^|\n]*|[^|\n]*|\\( *\\([^|\n]*\\) |\\).*$")
          (let* ((beg (match-beginning 0))
                 (end (match-end 0))
                 (line-overlay (make-overlay beg end))
                 (tag-overlay (make-overlay (match-beginning 1) (match-end 1)))
                 (tag-list (split-string (match-string 2) "_")))
            (overlay-put line-overlay 'help-echo tag-list)
            (overlay-put tag-overlay 'invisible t)
            (dolist (tag tag-list)
              (cond
               ((string-match "^Task/\\([0-9]\\)$" tag)
                (overlay-put tag-overlay 'after-string (make-string (string-to-number (match-string 1 tag))  ? )))
               ((member tag '("Error or warning" "Long operation" "Task" "Unique"))
                ;; handled by font lock
                )
               ((string= tag "VM, Host or Task ID")
                ;; generic tag without special handling
                )
               (t
                (add-to-list 'unknown-tags tag))))))
        (forward-line))
      (when unknown-tags
        (message "Unknown tags found: %s" (mapconcat #'identity unknown-tags "; "))))))

(defvar ovirt-log-analyzer-font-lock-keywords
  '((("[0-9a-f]\\{8\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{12\\}" 0 font-lock-variable-name-face t)
     ("\\<\\(Command\\|VM\\)\\>\\|FAILED\\|SUCCEEDED" 0 font-lock-keyword-face t)
     ("Error or warning.*$" 0 font-lock-warning-face keep)
     ("^\\(.*\\)|.*|.*Long operation" 1 font-lock-warning-face keep)
     ("^.*|\\(.*\\)|.*Unique" 1 font-lock-warning-face keep)
     ("Task.*|.*$" 0 font-lock-preprocessor-face keep))))

(defvar ovirt-log-analyzer-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map [return] 'ovirt-log-analyzer-show-log)
    (define-key map [tab] 'ovirt-log-analyzer-next-poi)
    (define-key map [backtab] 'ovirt-log-analyzer-previous-poi)
    (define-key map "a" 'ovirt-log-analyzer-toggle-filter)
    (define-key map "f" 'ovirt-log-analyzer-filter)
    (define-key map "n" 'next-logical-line)
    (define-key map "p" 'previous-logical-line)
    (define-key map "t" 'toggle-truncate-lines)
    map))

(define-derived-mode ovirt-log-analyzer-mode text-mode "OLA"
  (view-mode 1)
  (hl-line-mode 1)
  (toggle-truncate-lines 1)
  (setq font-lock-defaults ovirt-log-analyzer-font-lock-keywords)
  (ovirt-log-analyzer-process))

(provide 'ovirt-log-analyzer)
