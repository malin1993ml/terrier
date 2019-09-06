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
    /*
     * Use this class to store the setups of TPCH threads and cache the modules
     */
    class TplClass {
    public:
        // Terrier objects
        terrier::transaction::TransactionManager * txn_manager_pointer_;
        exec::SampleOutput * sample_output_pointer_;
        terrier::catalog::db_oid_t db_oid_;
        terrier::catalog::Catalog & catalog_;
        bool *unfinished_;

        // Map from file name to its module
        std::map <std::string, std::unique_ptr <vm::Module> > modules_;

        /*
         * Constructor
         */
        TplClass(terrier::transaction::TransactionManager * txn_manager_pointer,
                 exec::SampleOutput * sample_output_pointer,
                 terrier::catalog::db_oid_t db_oid,
                 terrier::catalog::Catalog & catalog,
                 bool *unfinished) :
                txn_manager_pointer_(txn_manager_pointer),
                sample_output_pointer_(sample_output_pointer),
                db_oid_(db_oid),
                catalog_(catalog),
                unfinished_(unfinished) {}

        /*
         * Run TPL from a file and record the time, mostly copied from tpl.cpp
         */
        void RunFile(const std::string &filename,
                double *interp_exec_ms_sum,
                uint64_t *interp_exec_ms_cnt,
                double *adaptive_exec_ms_sum,
                uint64_t *adaptive_exec_ms_cnt,
                double *jit_exec_ms_sum,
                uint64_t *jit_exec_ms_cnt,
                bool interp, bool adaptive, bool jit);

        /*
         * Signal handler
         */
        static void TplClassSignalHandler(i32 sig_num) {
            if (sig_num == SIGINT) {
                ShutdownTplClass();
                exit(0);
            }
        }

        static void BuildDb(terrier::transaction::TransactionManager &txn_manager,
                            terrier::storage::BlockStore &block_store,
                            exec::SampleOutput &sample_output,
                            terrier::catalog::db_oid_t &db_oid,
                            terrier::catalog::Catalog &catalog,
                            std::string db_name, std::string table_root);

        /*
         * Initialize the Terrier objects for TPCH
         */
        static int InitTplClass(int argc, char **argv);

        /*
         * Shutdown all TPL subsystems
         */
        static void ShutdownTplClass();
    };
}  // namespace tpl