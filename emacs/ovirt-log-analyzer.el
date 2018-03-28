;;; Major mode for inspecting oVirt log analyzer result output

(require 'cl-lib)
(require 'hl-line)

(defvar ovirt-log-analyzer-frequent-file "frequent.txt")

(defvar ovirt-log-analyzer-hosts '())
(make-variable-buffer-local 'ovirt-log-analyzer-hosts)

(defvar ovirt-log-analyzer-vms '())
(make-variable-buffer-local 'ovirt-log-analyzer-vms)

(defmacro ovirt-log-analyzer-while (interval condition &rest body)
  (let ((counter (make-symbol "counter")))
    `(let ((,counter 0))
       (while ,condition
         ,@body
         (cl-incf ,counter)
         (when (= (mod ,counter ,interval) 0)
           (accept-process-output)
           (sit-for 0))))))

(defun ovirt-log-analyzer-visible-p (point)
  (not (eq (get-char-property point 'invisible) 'yes)))

(defun ovirt-log-analyzer-file-reference ()
  (let ((file-reference (get-text-property (point) 'ovirt-log-analyzer-file-reference)))
    (unless file-reference
      (error "No file reference found"))
    (string-match "^ *\\(.*\\):\\([0-9]+\\)" file-reference)
    (list (match-string 1 file-reference)
          (string-to-number (match-string 2 file-reference)))))

(defun ovirt-log-analyzer-show-log ()
  (interactive)
  (cl-destructuring-bind (file line) (ovirt-log-analyzer-file-reference)
    (find-file file)
    (goto-char 1)
    (forward-line (1- line))))

(defun ovirt-log-analyzer-show-frequent-log ()
  (interactive)
  (unless (file-exists-p ovirt-log-analyzer-frequent-file)
    (error "%s file not present in the current directory" ovirt-log-analyzer-frequent-file))
  (cl-destructuring-bind (file line) (ovirt-log-analyzer-file-reference)
    (find-file ovirt-log-analyzer-frequent-file)
    (goto-char (point-min))
    (let ((regexp (concat "^[^|]*| *" (regexp-quote file) ":\\([0-9]+\\)"))
          (last-found (point))
          (distance (1+ line)))
      (while (re-search-forward regexp nil t)
        (let* ((frequent-line (string-to-number (match-string 1)))
               (frequent-distance (abs (- line frequent-line))))
          (if (>= frequent-distance distance)
              (goto-char (point-max))   ; end loop
            (setq last-found (point)
                  distance frequent-distance))))
      (goto-char last-found))))

(defun ovirt-log-analyzer-show-tags ()
  (interactive)
  (message "%s" (or (get-char-property (point) 'help-echo) "No tags")))

(defun ovirt-log-analyzer-poi (search-function limit-function)
  (let ((point (funcall search-function (point) 'face)))
    (while (and point
                (< point (point-max))
                (or (not (eq (get-text-property point 'face) 'font-lock-variable-name-face))
                    (not (ovirt-log-analyzer-visible-p point))))
      (setq point (funcall search-function point 'face nil (funcall limit-function))))
    (if (and point
             (< point (point-max)))
        (goto-char point)
      (message "No further point of interest"))))

(defun ovirt-log-analyzer-next-poi ()
  (interactive)
  (ovirt-log-analyzer-poi #'(lambda (&rest args) (apply #'next-single-property-change args))
                          #'point-max))

(defun ovirt-log-analyzer-previous-poi ()
  (interactive)
  (ovirt-log-analyzer-poi #'(lambda (&rest args) (apply #'previous-single-property-change args))
                          #'point-min))

(defun ovirt-log-analyzer-filter-by-function (filter-function)
  (save-excursion
    (goto-char (point-min))
    (ovirt-log-analyzer-while 500 (< (point) (point-max))
      (let ((beg (point))
            (end (if (funcall filter-function) (line-beginning-position) (point-max))))
        (unless (= beg end)
          (let ((o (make-overlay beg end)))
            (overlay-put o 'invisible 'yes)
            (overlay-put o 'priority 20)
            (overlay-put o 'ovirt-log-analyzer-filter t)))
        (goto-char end)
        (forward-line)))))

(defun ovirt-log-analyzer-filter-by-text (text)
  (message "Filtering by %s" text)
  (ovirt-log-analyzer-filter-by-function (lambda () (search-forward text nil t))))

(defun ovirt-log-analyzer-filter ()
  (interactive)
  (dolist (o (overlays-in (point-min) (point-max)))
    (when (overlay-get o 'ovirt-log-analyzer-filter)
      (delete-overlay o)))
  (cond
   ((or (eq (get-text-property (point) 'face) 'font-lock-variable-name-face))
    (ovirt-log-analyzer-filter-by-text
     (or (get-char-property (point) 'ovirt-log-analyzer-host)
         (get-char-property (point) 'ovirt-log-analyzer-vm)
         (buffer-substring-no-properties
          (if (eq (get-text-property (1- (point)) 'face) 'font-lock-variable-name-face)
              (previous-single-property-change (point) 'face)
            (point))
          (next-single-property-change (point) 'face)))))
   ((get-text-property (point) 'ovirt-log-analyzer-file-reference)
    (let ((file (car (ovirt-log-analyzer-file-reference))))
      (ovirt-log-analyzer-filter-by-function
       (lambda ()
         (while (and (< (point) (point-max))
                     (not (string= file (car (ignore-errors (ovirt-log-analyzer-file-reference))))))
           (goto-char (next-single-property-change (point) 'ovirt-log-analyzer-file-reference nil (point-max))))
         (< (point) (point-max))))))
   (t
    (error "Nothing to filter on"))))

(defun ovirt-log-analyzer-toggle-filter ()
  (interactive)
  (dolist (o (overlays-in (point-min) (point-max)))
    (when (overlay-get o 'ovirt-log-analyzer-filter)
      (let ((invisible (not (eq (overlay-get o 'invisible) 'yes))))
        (overlay-put o 'invisible (if invisible 'yes nil))
        (overlay-put o 'priority (if invisible 20 0))))))

(defun ovirt-log-analyzer-add-text (point string &optional face)
  (save-excursion
    (goto-char point)
    (insert (propertize string 'face face 'ovirt-log-analyzer-virtual t))))

(defun ovirt-log-analyzer-cleanup ()
  (let ((inhibit-read-only t))
    (remove-overlays)
    (remove-list-of-text-properties (point-min) (point-max)
                                    '(ovirt-log-analyzer-file
                                      ovirt-log-analyzer-file-field-length
                                      ovirt-log-analyzer-file-reference
                                      ovirt-log-analyzer-host
                                      ovirt-log-analyzer-vm
                                      after-string
                                      face
                                      help-echo
                                      invisible))
    (let ((point (point-min)))
      (while (< point (point-max))
        (let ((beg (next-single-property-change point 'ovirt-log-analyzer-virtual)))
          (if beg
              (let ((end (or (next-single-property-change beg 'ovirt-log-analyzer-virtual) (eobp))))
                (delete-region beg end)
                (setq point beg))
            (setq point (point-max))))))))

(defun ovirt-log-analyzer-process ()
  (save-excursion
    (goto-char (point-min))
    (let ((inhibit-read-only t)
          (modified-p (buffer-modified-p))
          (unknown-tags '())
          (max-file-field-length 0))
      (ovirt-log-analyzer-cleanup)
      (ovirt-log-analyzer-while 500 (not (eobp))
        (if (looking-at "[0-9][^|\n]*| \\( *[^|\n]+:[0-9]+\\) *| \\( *\\([^|\n]*\\) |\\).*$")
            (let* ((beg (match-beginning 0))
                   (end (match-end 0))
                   (file-point (match-beginning 1))
                   (tag-beg (match-beginning 2))
                   (tag-end (+ (match-end 2) 1))
                   (file-field (match-string 1))
                   (tag-field (match-string 3))
                   (tag-list (split-string (match-string 3) ";"))
                   (visible-tags nil))
              (add-text-properties beg end (list 'help-echo (mapconcat #'identity tag-list "; ")
                                                 'ovirt-log-analyzer-file-reference file-field
                                                 'ovirt-log-analyzer-file (car (split-string file-field ":"))))
              (when (string-match "^\\(.*/\\)\\([^/]+\\)$" file-field)
                (let ((length (- (match-end 2) (match-beginning 2))))
                  (add-text-properties file-point (+ file-point (match-end 1))
                                       (list 'invisible 'yes
                                             'ovirt-log-analyzer-file-field-length length))
                  (setq max-file-field-length (max max-file-field-length length))))
              (dolist (tag tag-list)
                (cond
                 ((string-match "^Task(duration=\\([0-9.]+\\))$" tag)
                  (ovirt-log-analyzer-add-text tag-end (concat (match-string 1 tag) "s ") 'font-lock-constant-face))
                 ((string-match "^Task/\\([1-9]\\)$" tag)
                  (ovirt-log-analyzer-add-text tag-end (make-string (string-to-number (match-string 1 tag)) ?>)
                                               'font-lock-keyword-face))
                 ((string-match "^Host=\\(.*\\)$" tag)
                  (let ((host (match-string 1 tag)))
                    (add-to-list 'ovirt-log-analyzer-hosts host)
                    (setq visible-tags t)))
                 ((string-match "^VM=\\(.*\\)$" tag)
                  (let ((vm (match-string 1 tag)))
                    (add-to-list 'ovirt-log-analyzer-vms vm)
                    (setq visible-tags t)))
                 ((member tag '("Error or warning" "Long operation" "Task" "Unique"))
                  ;; handled by font lock
                  )
                 ((string= tag "VM, Host or Task ID")
                  ;; generic tag without special handling
                  )
                 (t
                  (add-to-list 'unknown-tags tag))))
              (when visible-tags
                (goto-char tag-beg)
                (while (re-search-forward "\\(Host\\|VM\\)=\\([^; |]*\\)" tag-end t)
                  (let* ((beg (match-beginning 0))
                         (end (match-end 0))
                         (entity (match-string-no-properties 1))
                         (value (match-string-no-properties 2))
                         (property (cond
                                    ((string= entity "Host") 'ovirt-log-analyzer-host)
                                    ((string= entity "VM") 'ovirt-log-analyzer-vm)))
                         (text (concat entity "=" value ": ")))
                    (ovirt-log-analyzer-add-text tag-end text)
                    (put-text-property tag-end (+ tag-end (- (length text) 1)) property value)
                    (cl-incf end)
                    (goto-char end))))
              (put-text-property (- tag-beg 2) (- tag-end 2) 'invisible 'yes))
          (put-text-property (line-beginning-position) (1+ (line-end-position)) 'invisible 'yes))
        (forward-line))
      (when (> max-file-field-length 0)
        (goto-char (point-min))
        (let (point)
          (ovirt-log-analyzer-while 500 (and (not (eobp))
                                            (setq point (next-single-char-property-change (point) 'ovirt-log-analyzer-file-field-length)))
            (goto-char point)
            (let ((length (get-char-property (point) 'ovirt-log-analyzer-file-field-length)))
              (when (and length
                         (< length max-file-field-length)
                         (get-text-property (point) 'ovirt-log-analyzer-file-field-length))
                (let ((spacer (make-string (- max-file-field-length length) ? )))
                  (ovirt-log-analyzer-add-text (point) spacer)
                  (goto-char (+ (point) (length spacer))))))
            (goto-char (1+ (point))))))
      (unless modified-p
        (set-buffer-modified-p nil))
      (when unknown-tags
        (message "Unknown tags found: %s" (mapconcat #'identity unknown-tags "; "))))))

(defvar ovirt-log-analyzer-font-lock-keywords
  '((("[0-9a-f]\\{8\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{4\\}-[0-9a-f]\\{12\\}" 0 font-lock-variable-name-face t)
     ("\\<\\(Command\\|VM\\)\\>\\|FAILED\\|SUCCEEDED" 0 font-lock-keyword-face t)
     ("\\(Host\\|VM\\)=.*?:" 0 font-lock-variable-name-face t)
     ("Error or warning.*$" 0 font-lock-warning-face keep)
     ("^\\(.*\\)|.*|.*Task(duration=" 1 font-lock-warning-face keep)
     ("^.*|\\(.*\\)|.*Unique" 1 font-lock-warning-face keep)
     ("Task.*|.*$" 0 font-lock-preprocessor-face keep)
     ;; The following hack is necessary to break connections to invisible texts with the same face.
     ;; Simple faces such as `default' or `bold' don't work here.
     ("| " 0 font-lock-negation-char-face t))))

(defvar ovirt-log-analyzer-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map [return] 'ovirt-log-analyzer-show-log)
    (define-key map [(meta return)] 'ovirt-log-analyzer-show-frequent-log)
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
  (setq buffer-invisibility-spec '(yes))
  (add-hook 'before-save-hook 'ovirt-log-analyzer-cleanup nil t)
  (ovirt-log-analyzer-process))

(provide 'ovirt-log-analyzer)
