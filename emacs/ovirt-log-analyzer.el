;;; Major mode for inspecting oVirt log analyzer result output

(require 'cl-lib)
(require 'hl-line)

(defvar ovirt-log-analyzer-hosts '())
(make-variable-buffer-local 'ovirt-log-analyzer-hosts)

(defvar ovirt-log-analyzer-vms '())
(make-variable-buffer-local 'ovirt-log-analyzer-vms)

(defun ovirt-log-analyzer-show-log ()
  (interactive)
  (let ((file-reference (get-char-property (point) 'ovirt-log-analyzer-file-reference)))
    (unless file-reference
      (error "No file reference found"))
    (string-match "^ *\\(.*\\):\\([0-9]+\\)" file-reference)
    (let ((file (match-string 1 file-reference))
          (line (string-to-number (match-string 2 file-reference))))
      (find-file file)
      (goto-char 1)
      (forward-line (1- line)))))

(defun ovirt-log-analyzer-show-tags ()
  (interactive)
  (message "%s" (or (get-char-property (point) 'help-echo) "No tags")))

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

(defun ovirt-log-analyzer-filter-by-function (filter-function)
  (save-excursion
    (goto-char (point-min))
    (while (not (eobp))
      (unless (funcall filter-function)
        (let ((o (make-overlay (line-beginning-position) (1+ (line-end-position)))))
          (overlay-put o 'invisible t)
          (overlay-put o 'ovirt-log-analyzer-filter t)))
      (forward-line))))

(defun ovirt-log-analyzer-filter-by-text (text)
  (message "Filtering by %s" text)
  (ovirt-log-analyzer-filter-by-function (lambda () (search-forward text (line-end-position) t))))

(defun ovirt-log-analyzer-filter-by-property (property value)
  (message "Filtering by %s" value)
  (ovirt-log-analyzer-filter-by-function (lambda () (equal (get-char-property (point) property) value))))

(defun ovirt-log-analyzer-filter ()
  (interactive)
  (dolist (o (overlays-in (point-min) (point-max)))
    (when (overlay-get o 'ovirt-log-analyzer-filter)
      (delete-overlay o)))
  (let ((analyzer-properties '()))
    (dolist (property '(ovirt-log-analyzer-file ovirt-log-analyzer-host ovirt-log-analyzer-vm))
      (let ((value (get-char-property (point) property)))
        (when value
          (push value analyzer-properties)
          (push property analyzer-properties))))
    (cond
     ((eq (get-text-property (point) 'face) 'font-lock-variable-name-face)
      (ovirt-log-analyzer-filter-by-text
       (buffer-substring-no-properties (previous-single-property-change (point) 'face)
                                       (next-single-property-change (point) 'face))))
     (analyzer-properties
      (ovirt-log-analyzer-filter-by-property (cl-first analyzer-properties) (cl-second analyzer-properties)))
     (t
      (error "Nothing to filter on")))))

(defun ovirt-log-analyzer-toggle-filter ()
  (interactive)
  (dolist (o (overlays-in (point-min) (point-max)))
    (when (overlay-get o 'ovirt-log-analyzer-filter)
      (overlay-put o 'invisible (not (overlay-get o 'invisible))))))

(defun ovirt-log-analyzer-add-after (overlay string face)
  (overlay-put overlay 'after-string
               (concat (or (overlay-get overlay 'after-string) "")
                       (propertize string 'face face) " ")))

(defun ovirt-log-analyzer-process ()
  (save-excursion
    (goto-char (point-min))
    (remove-overlays)
    (let ((unknown-tags '())
          (max-file-field-length 0))
      (while (not (eobp))
        (point)
        (when (looking-at "[0-9][^|\n]*| \\( *[^|\n]+:[0-9]+\\) *|\\( *\\([^|\n]*\\) |\\).*$")
          (let* ((beg (match-beginning 0))
                 (end (match-end 0))
                 (line-overlay (make-overlay beg end))
                 (file-point (match-beginning 1))
                 (tag-overlay (make-overlay (match-beginning 2) (match-end 2)))
                 (file-field (match-string 1))
                 (tag-field (match-string 3))
                 (tag-list (split-string (match-string 3) "_")))
            (overlay-put line-overlay 'help-echo (mapconcat #'identity tag-list "; "))
            (overlay-put line-overlay 'ovirt-log-analyzer-file-reference file-field)
            (overlay-put line-overlay 'ovirt-log-analyzer-file (car (split-string file-field ":")))
            (when (string-match "^\\(.*/\\)\\([^/]+\\)$" file-field)
              (let ((file-overlay (make-overlay file-point (+ file-point (match-end 1))))
                    (length (- (match-end 2) (match-beginning 2))))
                (overlay-put file-overlay 'invisible t)
                (overlay-put file-overlay 'ovirt-log-analyzer-file-field-length length)
                (setq max-file-field-length (max max-file-field-length length))))
            (overlay-put tag-overlay 'invisible t)
            (dolist (tag tag-list)
              (cond
               ((string-match "^Task/\\([1-9]\\)$" tag)
                (overlay-put tag-overlay 'after-string (make-string (string-to-number (match-string 1 tag))  ? )))
               ((string-match "^Task(duration=\\([0-9.]+\\))$" tag)
                (ovirt-log-analyzer-add-after tag-overlay (concat (match-string 1 tag) "s" ) 'font-lock-constant-face))
               ((string-match "^Host=\\(.*\\)$" tag)
                (let ((host (match-string 1 tag)))
                  (add-to-list 'ovirt-log-analyzer-hosts host)
                  (ovirt-log-analyzer-add-after tag-overlay (match-string 1 tag) 'font-lock-variable-name-face)
                  (overlay-put line-overlay 'ovirt-log-analyzer-host host)))
               ((string-match "^VM=\\(.*\\)$" tag)
                (let ((vm (match-string 1 tag)))
                  (add-to-list 'ovirt-log-analyzer-vms vm)
                  (ovirt-log-analyzer-add-after tag-overlay (match-string 1 tag) 'font-lock-variable-name-face)
                  (overlay-put line-overlay 'ovirt-log-analyzer-vm vm)))
               ((member tag '("Error or warning" "Long operation" "Task" "Unique"))
                ;; handled by font lock
                )
               ((string= tag "VM, Host or Task ID")
                ;; generic tag without special handling
                )
               (t
                (add-to-list 'unknown-tags tag))))))
        (forward-line))
      (when (> max-file-field-length 0)
        (goto-char (point-min))
        (let (point)
          (while (and (not (eobp))
                      (setq point (next-single-char-property-change (point) 'ovirt-log-analyzer-file-field-length)))
            (goto-char point)
            (let ((length (get-char-property (point) 'ovirt-log-analyzer-file-field-length)))
              (when (and length (< length max-file-field-length))
                (let ((overlay (cl-find-if #'(lambda (o) (overlay-get o 'ovirt-log-analyzer-file-field-length)) (overlays-at (point)))))
                  (overlay-put overlay 'after-string (make-string (- max-file-field-length length) ? )))))
            (goto-char (1+ (point))))))
      (when unknown-tags
        (message "Unknown tags found: %s" (mapconcat #'identity unknown-tags "; "))))))

(defvar ovirt-log-analyzer-font-lock-keywords
  '((("[0-9a-f]\\{8\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{12\\}" 0 font-lock-variable-name-face t)
     ("\\<\\(Command\\|VM\\)\\>\\|FAILED\\|SUCCEEDED" 0 font-lock-keyword-face t)
     ("Error or warning.*$" 0 font-lock-warning-face keep)
     ("^\\(.*\\)|.*|.*Task(duration=" 1 font-lock-warning-face keep)
     ("^.*|\\(.*\\)|.*Unique" 1 font-lock-warning-face keep)
     ("Task.*|.*$" 0 font-lock-preprocessor-face keep))))

(defvar ovirt-log-analyzer-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map [return] 'ovirt-log-analyzer-show-log)
    (define-key map [tab] 'ovirt-log-analyzer-next-poi)
    (define-key map [backtab] 'ovirt-log-analyzer-previous-poi)
    (define-key map "a" 'ovirt-log-analyzer-toggle-filter)
    (define-key map "f" 'ovirt-log-analyzer-filter)
    (define-key map [(meta n)] 'next-logical-line)
    (define-key map [(meta p)] 'previous-logical-line)
    (define-key map "t" 'ovirt-log-analyzer-show-tags)
    (define-key map "T" 'toggle-truncate-lines)
    map))

(define-derived-mode ovirt-log-analyzer-mode text-mode "OLA"
  (view-mode 1)
  (hl-line-mode 1)
  (toggle-truncate-lines 1)
  (setq font-lock-defaults ovirt-log-analyzer-font-lock-keywords)
  (ovirt-log-analyzer-process))

(provide 'ovirt-log-analyzer)
