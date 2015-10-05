(in-package :cl-bunny.examples)


;; http://christophe.rhodes.io/notes/blog/posts/2014/code_walking_for_pipe_sequencing/
(defmacro -> (&body forms)
  (let ((rewrite t))
    (declare (special rewrite))
    (labels ((find-$ (form env)
               (sb-walker:walk-form form env
                (lambda (f c e)
                  (cond
                    ((and (eql f '$) (eql c :eval))
                     (return-from find-$ t))
                    (t f))))
               nil)
             (walker (form context env)
               (declare (ignore context))
               (typecase form
                 (symbol (if rewrite (list form) form))
                 (atom form)
                 ((cons (member with-rewriting without-rewriting))
                  (let ((rewrite (eql (car form) 'with-rewriting)))
                    (declare (special rewrite))
                    (values (sb-walker:walk-form (cadr form) env #'walker) t)))
                 ((cons (member let let*))
                  (unless rewrite
                    (return-from walker form))
                  (let* ((body (member 'declare (cddr form)
                                       :key (lambda (x) (when (consp x) (car x))) :test-not #'eql))
                         (declares (ldiff (cddr form) body))
                         (rewritten (sb-walker:walk-form
                                     `(without-rewriting
                                          (,(car form) ,(cadr form)
                                            ,@declares
                                            (with-rewriting
                                                ,@body)))
                                     env #'walker)))
                    (values rewritten t)))
                 (t
                  (unless rewrite
                    (return-from walker form))
                  (if (find-$ form env)
                      (values `(setq $ ,form) t)
                      (values `(setq $ ,(list* (car form) '$ (cdr form))) t))))))
      `(let (($ ,(car forms)))
         ,@(mapcar (lambda (f) (sb-walker:walk-form f nil #'walker)) (cdr forms))))))
