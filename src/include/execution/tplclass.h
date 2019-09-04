namespace terrier::execution::exec {
    class SampleOutput;
}

namespace terrier::catalog {
    class Catalog;
}

namespace terrier::execution::vm {
    class Module;
}

namespace terrier::execution {
    class TplClass {
    public:
        // Terrier objects
        terrier::transaction::TransactionManager * txn_manager_pointer_;
        exec::SampleOutput * sample_output_pointer_;
        terrier::catalog::db_oid_t db_oid_;
        terrier::catalog::Catalog * catalog_pointer_;
        bool *unfinished_;

        std::map <std::string, std::unique_ptr <vm::Module> > modules_;

        TplClass(terrier::transaction::TransactionManager * txn_manager_pointer,
                 exec::SampleOutput * sample_output_pointer,
                 terrier::catalog::db_oid_t db_oid,
                 terrier::catalog::Catalog * catalog_pointer,
                 bool *unfinished) :
                txn_manager_pointer_(txn_manager_pointer),
                sample_output_pointer_(sample_output_pointer),
                db_oid_(db_oid),
                catalog_pointer_(catalog_pointer),
                unfinished_(unfinished) {}

        void RunFile(const std::string &filename,
                double *interp_exec_ms_sum,
                uint64_t *interp_exec_ms_cnt,
                double *adaptive_exec_ms_sum,
                uint64_t *adaptive_exec_ms_cnt,
                double *jit_exec_ms_sum,
                uint64_t *jit_exec_ms_cnt,
                bool interp, bool adaptive, bool jit);
    };

    int InitTplClass(int argc, char **argv,
                     terrier::transaction::TransactionManager &txn_manager,
                     terrier::storage::BlockStore &block_store,
                     exec::SampleOutput &sample_output,
                     terrier::catalog::db_oid_t &db_oid,
                     terrier::catalog::Catalog &catalog);

    void ShutdownTplClass();

}  // namespace tpl