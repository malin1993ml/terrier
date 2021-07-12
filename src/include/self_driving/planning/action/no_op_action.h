#pragma once

#include <string>

#include "self_driving/planning/action/abstract_action.h"

namespace noisepage::selfdriving::pilot {

class ActionState;

/**
 * Represent a no-op action
 */
class NoOpAction : public AbstractAction {
 public:
  /**
   * Construct NoOpAction
   * @param db_oid Database id
   */
  NoOpAction() : AbstractAction(ActionType::NO_OP, catalog::INVALID_DATABASE_OID) { sql_command_ = ";"; }

  void ModifyActionState(ActionState *action_state) override{};
};

}  // namespace noisepage::selfdriving::pilot
