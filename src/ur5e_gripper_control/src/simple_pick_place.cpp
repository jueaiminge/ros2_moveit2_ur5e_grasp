#include <algorithm>
#include <chrono>
#include <cmath>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <control_msgs/action/gripper_command.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <moveit/move_group_interface/move_group_interface.h>
#include <moveit/planning_scene_interface/planning_scene_interface.h>
#include <moveit/robot_state/conversions.h>
#include <moveit_msgs/msg/allowed_collision_entry.hpp>
#include <moveit_msgs/msg/attached_collision_object.hpp>
#include <moveit_msgs/msg/collision_object.hpp>
#include <moveit_msgs/msg/display_trajectory.hpp>
#include <moveit_msgs/msg/planning_scene.hpp>
#include <moveit_msgs/msg/planning_scene_components.hpp>
#include <moveit_msgs/srv/get_planning_scene.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp_action/rclcpp_action.hpp>
#include <shape_msgs/msg/solid_primitive.hpp>
#include <std_msgs/msg/string.hpp>
#include <tf2/LinearMath/Quaternion.h>

using namespace std::chrono_literals;

class SimplePickPlaceNode : public rclcpp::Node
{
  using GripperCommand = control_msgs::action::GripperCommand;

  struct ArmConfig
  {
    std::string label;
    std::string arm_group;
    std::string hand_group;
    std::string hand_frame;
    std::string home_pose_name;
    std::string object_id;
    std::string gripper_action_name;
    std::vector<double> source_position;
    std::vector<double> target_position;
    std::vector<double> source_position_offset;
    std::vector<double> pick_yaw_candidates;
    std::vector<double> place_yaw_candidates;
    double place_yaw{};
  };

  struct ArmRuntime
  {
    ArmConfig config;
    std::shared_ptr<moveit::planning_interface::MoveGroupInterface> move_group;
    rclcpp_action::Client<GripperCommand>::SharedPtr gripper_action_client;
  };

  struct PlannedStage
  {
    ArmRuntime* arm;
    moveit::planning_interface::MoveGroupInterface::Plan plan;
    std::string stage_name;
    double start_delay_sec{ 0.0 };
    bool allow_risk_response{ false };
    bool allow_warn_slowdown{ false };
  };

  enum class RiskAction
  {
    NONE,
    SLOWDOWN,
    STOP,
    REPLAN,
  };

  struct MonitorRiskState
  {
    std::string risk{ "safe" };
    std::string action{ "none" };
    std::string scope{ "unknown" };
    std::string first_body;
    std::string second_body;
    double future_offset{ 0.0 };
    rclcpp::Time received_at{ 0, 0, RCL_ROS_TIME };
  };

  struct RunningStageState
  {
    std::mutex mutex;
    bool started{ false };
    bool slowdown_applied{ false };
    rclcpp::Time execution_start_time{ 0, 0, RCL_ROS_TIME };
  };

  struct StageExecutionResult
  {
    bool succeeded{ false };
    bool slowdown_applied{ false };
    RiskAction interruption_action{ RiskAction::NONE };
  };

public:
  explicit SimplePickPlaceNode(const rclcpp::NodeOptions& options)
  : Node("simple_pick_place", options)
  {
    world_frame_ = declare_parameter<std::string>("world_frame", "world");
    table_id_ = declare_parameter<std::string>("table_id", "table");
    object_size_ = declare_parameter<std::vector<double>>("object_size", { 0.05, 0.05, 0.05 });
    table_position_ = declare_parameter<std::vector<double>>("table_position", { 0.0, 0.0, 1.0 });
    table_size_ = declare_parameter<std::vector<double>>("table_size", { 1.8, 0.8, 0.03 });

    left_arm_.config.label = "left";
    left_arm_.config.object_id = declare_parameter<std::string>("object_id", "blue_box");
    left_arm_.config.arm_group = declare_parameter<std::string>("arm_group", "ur_manipulator");
    left_arm_.config.hand_group = declare_parameter<std::string>("hand_group", "gripper");
    left_arm_.config.hand_frame = declare_parameter<std::string>("hand_frame", "tool0");
    left_arm_.config.home_pose_name = declare_parameter<std::string>("home_pose_name", "ready");
    left_arm_.config.gripper_action_name = declare_parameter<std::string>(
      "gripper_action_name", "/gripper_controller/gripper_cmd");
    left_arm_.config.source_position =
      declare_parameter<std::vector<double>>("source_position", { 0.1, 0.1, 1.04 });
    left_arm_.config.target_position =
      declare_parameter<std::vector<double>>("target_position", { -0.15, -0.25, 1.06 });
    left_arm_.config.source_position_offset = declare_parameter<std::vector<double>>(
      "source_position_offset", { 0.0, 0.0, 0.0 });
    left_arm_.config.pick_yaw_candidates = declare_parameter<std::vector<double>>(
      "pick_yaw_candidates", { 0.0, 1.57079632679, -1.57079632679 });
    left_arm_.config.place_yaw = declare_parameter<double>("place_yaw", 0.0);
    left_arm_.config.place_yaw_candidates = declare_parameter<std::vector<double>>(
      "place_yaw_candidates", { left_arm_.config.place_yaw });

    right_arm_.config.label = "right";
    right_arm_.config.object_id = declare_parameter<std::string>("right_object_id", "red_box");
    right_arm_.config.arm_group =
      declare_parameter<std::string>("right_arm_group", "right_ur_manipulator");
    right_arm_.config.hand_group = declare_parameter<std::string>("right_hand_group", "right_gripper");
    right_arm_.config.hand_frame = declare_parameter<std::string>("right_hand_frame", "right_tool0");
    right_arm_.config.home_pose_name =
      declare_parameter<std::string>("right_home_pose_name", "right_ready");
    right_arm_.config.gripper_action_name = declare_parameter<std::string>(
      "right_gripper_action_name", "/right_gripper_controller/gripper_cmd");
    right_arm_.config.source_position = declare_parameter<std::vector<double>>(
      "right_source_position", { -0.1, 0.1, 1.04 });
    right_arm_.config.target_position = declare_parameter<std::vector<double>>(
      "right_target_position", { 0.15, -0.25, 1.06 });
    right_arm_.config.source_position_offset = declare_parameter<std::vector<double>>(
      "right_source_position_offset", { 0.0, 0.0, 0.0 });
    right_arm_.config.pick_yaw_candidates = declare_parameter<std::vector<double>>(
      "right_pick_yaw_candidates", { 0.0, 1.57079632679, -1.57079632679 });
    right_arm_.config.place_yaw = declare_parameter<double>("right_place_yaw", 0.0);
    right_arm_.config.place_yaw_candidates = declare_parameter<std::vector<double>>(
      "right_place_yaw_candidates", { right_arm_.config.place_yaw, 1.57079632679, -1.57079632679 });

    ignore_right_arm_collisions_ = declare_parameter<bool>("ignore_right_arm_collisions", false);
    enable_master_slave_sequencing_ =
      declare_parameter<bool>("enable_master_slave_sequencing", false);
    move_master_home_before_slave_resume_ =
      declare_parameter<bool>("move_master_home_before_slave_resume", true);
    enable_fcl_monitor_publisher_ = declare_parameter<bool>("enable_fcl_monitor_publisher", true);
    monitor_trajectory_topic_ =
      declare_parameter<std::string>("monitor_trajectory_topic", "/fcl_monitor/display_trajectory");
    monitor_risk_topic_ =
      declare_parameter<std::string>("monitor_risk_topic", "/fcl_monitor/risk_state");
    monitor_trajectory_publish_lead_time_ =
      declare_parameter<double>("monitor_trajectory_publish_lead_time", 0.05);
    enable_monitor_risk_replan_ =
      declare_parameter<bool>("enable_monitor_risk_replan", true);
    monitor_risk_timeout_ = declare_parameter<double>("monitor_risk_timeout", 0.5);
    max_right_arm_replan_attempts_ =
      declare_parameter<int>("max_right_arm_replan_attempts", 2);
    enable_right_local_wait_recovery_ =
      declare_parameter<bool>("enable_right_local_wait_recovery", true);
    right_local_wait_pose_name_ =
      declare_parameter<std::string>("right_local_wait_pose_name", "right_local_wait");
    right_local_wait_lift_distance_ =
      declare_parameter<double>("right_local_wait_lift_distance", 0.08);
    right_local_wait_position_offset_ = declare_parameter<std::vector<double>>(
      "right_local_wait_position_offset", { 0.0, 0.0, 0.0 });
    right_local_wait_planning_delay_sec_ =
      declare_parameter<double>("right_local_wait_planning_delay_sec", 0.45);
    right_resume_planning_delay_sec_ =
      declare_parameter<double>("right_resume_planning_delay_sec", 0.35);
    left_priority_clear_position_offset_ = declare_parameter<std::vector<double>>(
      "left_priority_clear_position_offset", { 0.10, 0.08, 0.0 });
    enable_staggered_dual_arm_execution_ =
      declare_parameter<bool>("enable_staggered_dual_arm_execution", false);
    primary_start_arm_ = declare_parameter<std::string>("primary_start_arm", "right");
    secondary_start_delay_ = declare_parameter<double>("secondary_start_delay", 2.0);
    warn_velocity_scale_ = declare_parameter<double>("warn_velocity_scale", 0.15);
    warn_acceleration_scale_ = declare_parameter<double>("warn_acceleration_scale", 0.15);
    max_named_target_retries_ = declare_parameter<int>("max_named_target_retries", 3);
    right_replan_fallback_to_home_ =
      declare_parameter<bool>("right_replan_fallback_to_home", true);

    grasp_height_offset_ = declare_parameter<double>("grasp_height_offset", 0.14);
    pregrasp_offset_ = declare_parameter<double>("pregrasp_offset", 0.06);
    lift_distance_ = declare_parameter<double>("lift_distance", 0.12);
    retreat_distance_ = declare_parameter<double>("retreat_distance", 0.08);
    planning_time_ = declare_parameter<double>("planning_time", 5.0);
    planning_attempts_ = declare_parameter<int>("planning_attempts", 5);
    max_velocity_scaling_ = declare_parameter<double>("max_velocity_scaling", 0.3);
    max_acceleration_scaling_ = declare_parameter<double>("max_acceleration_scaling", 0.3);
    gripper_open_position_ = declare_parameter<double>("gripper_open_position", 0.0);
    gripper_closed_position_ = declare_parameter<double>("gripper_closed_position", 0.36);
    return_home_after_place_ = declare_parameter<bool>("return_home_after_place", true);
    planner_id_ = declare_parameter<std::string>("planner_id", "RRTConnectkConfigDefault");
    cartesian_eef_step_ = declare_parameter<double>("cartesian_eef_step", 0.01);
    cartesian_jump_threshold_ = declare_parameter<double>("cartesian_jump_threshold", 0.0);
    cartesian_min_fraction_ = declare_parameter<double>("cartesian_min_fraction", 0.95);
    workspace_min_x_ = declare_parameter<double>("workspace_min_x", -0.55);
    workspace_max_x_ = declare_parameter<double>("workspace_max_x", 0.55);
    workspace_min_y_ = declare_parameter<double>("workspace_min_y", -0.35);
    workspace_max_y_ = declare_parameter<double>("workspace_max_y", 0.35);
    workspace_min_z_ = declare_parameter<double>("workspace_min_z", 1.02);
    workspace_max_z_ = declare_parameter<double>("workspace_max_z", 1.45);

    planning_scene_client_ =
      create_client<moveit_msgs::srv::GetPlanningScene>("/get_planning_scene");
    monitored_trajectory_publisher_ =
      create_publisher<moveit_msgs::msg::DisplayTrajectory>(monitor_trajectory_topic_, 10);
    monitor_risk_subscription_ = create_subscription<std_msgs::msg::String>(
      monitor_risk_topic_, rclcpp::SystemDefaultsQoS(),
      std::bind(&SimplePickPlaceNode::handleMonitorRiskState, this, std::placeholders::_1));
  }

  void init()
  {
    left_arm_.move_group = std::make_shared<moveit::planning_interface::MoveGroupInterface>(
      shared_from_this(), left_arm_.config.arm_group);
    right_arm_.move_group = std::make_shared<moveit::planning_interface::MoveGroupInterface>(
      shared_from_this(), right_arm_.config.arm_group);
    left_arm_.gripper_action_client =
      rclcpp_action::create_client<GripperCommand>(this, left_arm_.config.gripper_action_name);
    right_arm_.gripper_action_client =
      rclcpp_action::create_client<GripperCommand>(this, right_arm_.config.gripper_action_name);

    configureMoveGroup(left_arm_);
    configureMoveGroup(right_arm_);
  }

  bool run()
  {
    if (!validateParameters()) {
      return false;
    }

    if (!waitForCurrentState()) {
      return false;
    }

    if (ignore_right_arm_collisions_) {
      allowLeftArmAgainstRightArm(true);
    }

    setupPlanningScene();

    if (!moveBothHome()) {
      return false;
    }

    if (!commandGrippers({
          { &left_arm_, gripper_open_position_ },
          { &right_arm_, gripper_open_position_ },
        })) {
      return false;
    }

    if (!executeDualPickPlace()) {
      return false;
    }

    if (return_home_after_place_ && !settleAfterPlace()) {
      RCLCPP_WARN(
        get_logger(),
        "Pick-and-place finished, but one or more post-place return-to-ready motions failed. Keeping the completed task result.");
    }

    RCLCPP_INFO(get_logger(), "Dual-arm MoveIt2 pick and place finished successfully.");
    return true;
  }

private:
  void configureMoveGroup(ArmRuntime& arm)
  {
    arm.move_group->setPoseReferenceFrame(world_frame_);
    arm.move_group->setEndEffectorLink(arm.config.hand_frame);
    arm.move_group->setPlanningTime(planning_time_);
    arm.move_group->setNumPlanningAttempts(planning_attempts_);
    arm.move_group->setMaxVelocityScalingFactor(max_velocity_scaling_);
    arm.move_group->setMaxAccelerationScalingFactor(max_acceleration_scaling_);
    arm.move_group->setPlannerId(planner_id_);
    arm.move_group->setWorkspace(
      workspace_min_x_,
      workspace_min_y_,
      workspace_min_z_,
      workspace_max_x_,
      workspace_max_y_,
      workspace_max_z_);
    arm.move_group->allowReplanning(true);
  }

  bool validateParameters() const
  {
    return object_size_.size() == 3 && table_position_.size() == 3 && table_size_.size() == 3 &&
           hasPoseVector(left_arm_.config.source_position) && hasPoseVector(left_arm_.config.target_position) &&
           hasPoseVector(left_arm_.config.source_position_offset) &&
           hasPoseVector(right_arm_.config.source_position) && hasPoseVector(right_arm_.config.target_position) &&
           hasPoseVector(right_arm_.config.source_position_offset) &&
           hasPoseVector(right_local_wait_position_offset_) &&
           hasPoseVector(left_priority_clear_position_offset_);
  }

  bool hasPoseVector(const std::vector<double>& values) const
  {
    return values.size() == 3;
  }

  bool waitForCurrentState()
  {
    for (int attempt = 0; attempt < 30; ++attempt) {
      if (left_arm_.move_group->getCurrentState(1.0) && right_arm_.move_group->getCurrentState(1.0)) {
        return true;
      }
      RCLCPP_INFO_THROTTLE(
        get_logger(), *get_clock(), 2000, "Waiting for current dual-arm robot state from MoveIt...");
    }
    RCLCPP_ERROR(get_logger(), "Failed to receive current robot state for both arms.");
    return false;
  }

  void setupPlanningScene()
  {
    std::vector<moveit_msgs::msg::CollisionObject> collision_objects;
    collision_objects.push_back(makeBoxObject(table_id_, table_position_, table_size_));
    collision_objects.push_back(
      makeBoxObject(left_arm_.config.object_id, left_arm_.config.source_position, object_size_));
    collision_objects.push_back(
      makeBoxObject(right_arm_.config.object_id, right_arm_.config.source_position, object_size_));
    planning_scene_interface_.applyCollisionObjects(collision_objects);
    std::this_thread::sleep_for(500ms);
  }

  bool moveBothHome()
  {
    moveit::planning_interface::MoveGroupInterface::Plan left_plan;
    moveit::planning_interface::MoveGroupInterface::Plan right_plan;
    if (!planToNamedTarget(left_arm_, left_arm_.config.home_pose_name, left_plan) ||
        !planToNamedTarget(right_arm_, right_arm_.config.home_pose_name, right_plan)) {
      return false;
    }

    return executePlannedStages({
      {
        &left_arm_,
        left_plan,
        left_arm_.config.label + " move to named target: " + left_arm_.config.home_pose_name,
        stageDelayForArm(left_arm_),
      },
      {
        &right_arm_,
        right_plan,
        right_arm_.config.label + " move to named target: " + right_arm_.config.home_pose_name,
        stageDelayForArm(right_arm_),
      },
    });
  }

  bool settleAfterPlace()
  {
    if (moveBothHome()) {
      return true;
    }

    RCLCPP_WARN(
      get_logger(),
      "Concurrent return-to-ready failed after place. Will retry each arm individually without failing the completed pick-and-place task.");

    bool all_ok = true;
    if (!moveArmToNamedTarget(
          left_arm_, left_arm_.config.home_pose_name,
          "left post-place return to named target: " + left_arm_.config.home_pose_name)) {
      all_ok = false;
    }
    if (!moveArmToNamedTarget(
          right_arm_, right_arm_.config.home_pose_name,
          "right post-place return to named target: " + right_arm_.config.home_pose_name)) {
      all_ok = false;
    }
    return all_ok;
  }

  double stageDelayForArm(const ArmRuntime& arm) const
  {
    if (!enable_staggered_dual_arm_execution_) {
      return 0.0;
    }

    const std::string normalized_primary =
      (primary_start_arm_ == "left" || primary_start_arm_ == "right") ? primary_start_arm_ : "right";
    return arm.config.label == normalized_primary ? 0.0 : std::max(0.0, secondary_start_delay_);
  }

  bool executeConcurrentDualPickPlace()
  {
    if (!ensureObjectPresentInWorld(left_arm_.config.object_id, left_arm_.config.source_position) ||
        !ensureObjectPresentInWorld(right_arm_.config.object_id, right_arm_.config.source_position)) {
      return false;
    }
    std::this_thread::sleep_for(300ms);

    const auto left_pick_position = addOffset(
      left_arm_.config.source_position, left_arm_.config.source_position_offset);
    const auto right_pick_position = addOffset(
      right_arm_.config.source_position, right_arm_.config.source_position_offset);

    double left_pick_yaw = 0.0;
    double right_pick_yaw = 0.0;
    bool left_pick_completed_by_priority = false;
    if (!executeConcurrentPregraspWithLeftPriorityClear(
          [this, &left_pick_position, &left_pick_yaw](
            moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planPickPregrasp(left_arm_, left_pick_position, left_pick_yaw, plan);
          },
          [this, &right_pick_position, &right_pick_yaw](
            moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planPickPregrasp(right_arm_, right_pick_position, right_pick_yaw, plan);
          },
          left_pick_completed_by_priority)) {
      return false;
    }

    if (!left_pick_completed_by_priority) {
      return executeConcurrentArmSequences(
        [this]() { return executeArmPostPregraspSequence(left_arm_, "continuous"); },
        [this]() { return executeArmPostPregraspSequence(right_arm_, "continuous"); });
    }

    return executeConcurrentArmSequences(
      [this]() { return executeArmPlaceAndRetreatSequence(left_arm_, "priority continuation"); },
      [this, &right_pick_position, &right_pick_yaw]() {
        return executeRightArmResumePickPlaceWithFallback(
          right_pick_position, right_pick_yaw, "priority resume");
      });
  }

  bool executeDualPickPlace()
  {
    if (enable_master_slave_sequencing_) {
      RCLCPP_WARN(
        get_logger(),
        "Master/slave safety mode is enabled. The left arm will complete the full pick-and-place first, then the right arm resumes.");

      if (!executeSingleArmPickPlace(left_arm_)) {
        return false;
      }

      if (move_master_home_before_slave_resume_ &&
          !moveArmToNamedTarget(
            left_arm_, left_arm_.config.home_pose_name, "left return home before right resume")) {
        return false;
      }

      return executeSingleArmPickPlace(right_arm_);
    }
    return executeConcurrentDualPickPlace();
  }

  bool executeConcurrentPregraspWithLeftPriorityClear(
    const std::function<bool(moveit::planning_interface::MoveGroupInterface::Plan&)>& left_planner,
    const std::function<bool(moveit::planning_interface::MoveGroupInterface::Plan&)>& right_planner,
    bool& left_pick_completed_by_priority)
  {
    left_pick_completed_by_priority = false;
    const std::string left_stage_name = "left move to pregrasp concurrent";
    const std::string right_stage_name = "right move to pregrasp concurrent";
    const int plan_retries = std::max(1, max_named_target_retries_);
    moveit::planning_interface::MoveGroupInterface::Plan left_plan;
    moveit::planning_interface::MoveGroupInterface::Plan right_plan;
    bool planned = false;
    for (int attempt = 1; attempt <= plan_retries; ++attempt) {
      if (left_planner(left_plan) && right_planner(right_plan)) {
        planned = true;
        break;
      }
    }
    if (!planned) {
      return executeStageWithSafetyReplan(left_arm_, left_stage_name, left_planner) &&
             executeStageWithSafetyReplan(right_arm_, right_stage_name, right_planner);
    }

    const auto results = executePlannedStagesDetailed({
      { &left_arm_, left_plan, left_stage_name, stageDelayForArm(left_arm_) },
      { &right_arm_, right_plan, right_stage_name, stageDelayForArm(right_arm_), true, true },
    });
    if (results.size() != 2U) {
      return false;
    }
    if (results[0].succeeded && results[1].succeeded) {
      return true;
    }

    if (enable_right_local_wait_recovery_ && !results[1].succeeded &&
        (results[1].interruption_action == RiskAction::STOP ||
         results[1].interruption_action == RiskAction::REPLAN)) {
      RCLCPP_WARN(
        get_logger(),
        "Concurrent pregrasp was interrupted. Right arm will wait, and the left arm will clear the full pick critical segment first.");

      if (!moveRightArmToLocalWait(right_stage_name)) {
        RCLCPP_WARN(
          get_logger(),
          "Right arm could not reach a dedicated local wait pose for stage '%s'. It will hold the current stopped pose while the left arm clears the critical segment.",
          right_stage_name.c_str());
      }
      if (!results[0].succeeded &&
          !executeStageWithSafetyReplan(left_arm_, left_stage_name, left_planner)) {
        return false;
      }
      if (!executeLeftPriorityPickThroughLift()) {
        return false;
      }
      left_pick_completed_by_priority = true;

      return true;
    }

    if (!results[0].succeeded &&
        !executeStageWithSafetyReplan(left_arm_, left_stage_name, left_planner)) {
      return false;
    }
    if (!results[1].succeeded &&
        !executeStageWithSafetyReplan(right_arm_, right_stage_name, right_planner)) {
      return false;
    }
    return true;
  }

  bool executeLeftPriorityPickThroughLift()
  {
    if (!executeStageWithSafetyReplan(
          left_arm_, "left move to grasp priority recovery",
          [this](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planLinearOffset(
              left_arm_, 0.0, 0.0, -pregrasp_offset_, "left move to grasp priority recovery", plan);
          }) ||
        !commandGrippers({ { &left_arm_, gripper_closed_position_ } }) ||
        !attachObjectAtPose(left_arm_, left_arm_.config.source_position)) {
      return false;
    }

    setAllowedCollision(left_arm_.config.object_id, table_id_, true);
    if (!executeStageWithSafetyReplan(
      left_arm_, "left lift object priority recovery",
      [this](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
        return planLinearOffset(left_arm_, 0.0, 0.0, lift_distance_, "left lift object priority recovery", plan);
      })) {
      return false;
    }

    if (!hasMeaningfulOffset(left_priority_clear_position_offset_)) {
      return true;
    }

    return executeStageWithSafetyReplan(
      left_arm_, "left clear resume corridor priority recovery",
      [this](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
        return planToCurrentPoseOffset(
          left_arm_, left_priority_clear_position_offset_,
          "left clear resume corridor priority recovery", plan);
      });
  }

  bool executeConcurrentArmSequences(
    const std::function<bool()>& left_sequence, const std::function<bool()>& right_sequence,
    double left_start_delay_sec = 0.0, double right_start_delay_sec = 0.0)
  {
    auto launch_sequence = [](double start_delay_sec, const std::function<bool()>& sequence) {
      return std::async(std::launch::async, [start_delay_sec, sequence]() {
        if (start_delay_sec > 1e-3) {
          std::this_thread::sleep_for(std::chrono::duration<double>(start_delay_sec));
        }
        return sequence();
      });
    };

    auto left_future = launch_sequence(left_start_delay_sec, left_sequence);
    auto right_future = launch_sequence(right_start_delay_sec, right_sequence);
    const bool left_ok = left_future.get();
    const bool right_ok = right_future.get();
    return left_ok && right_ok;
  }

  bool executeArmPostPregraspSequence(ArmRuntime& arm, const std::string& tag)
  {
    const std::string prefix = arm.config.label + " ";
    if (!executeStageWithSafetyReplan(
          arm, prefix + "move to grasp " + tag,
          [this, &arm, &prefix, &tag](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planLinearOffset(
              arm, 0.0, 0.0, -pregrasp_offset_, prefix + "move to grasp " + tag, plan);
          }) ||
        !commandGrippers({ { &arm, gripper_closed_position_ } }) ||
        !attachObjectAtPose(arm, arm.config.source_position)) {
      return false;
    }

    setAllowedCollision(arm.config.object_id, table_id_, true);
    if (!executeStageWithSafetyReplan(
          arm, prefix + "lift object " + tag,
          [this, &arm, &prefix, &tag](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planLinearOffset(arm, 0.0, 0.0, lift_distance_, prefix + "lift object " + tag, plan);
          })) {
      return false;
    }

    return executeArmPlaceAndRetreatSequence(arm, tag);
  }

  bool executeArmPlaceAndRetreatSequence(ArmRuntime& arm, const std::string& tag)
  {
    const std::string prefix = arm.config.label + " ";
    if (!executeStageWithSafetyReplan(
          arm, prefix + "move to pre-place " + tag,
          [this, &arm](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planPlacePreplace(arm, plan);
          }) ||
        !executeStageWithSafetyReplan(
          arm, prefix + "lower to place " + tag,
          [this, &arm, &prefix, &tag](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planLinearOffset(
              arm, 0.0, 0.0, -pregrasp_offset_, prefix + "lower to place " + tag, plan);
          }) ||
        !commandGrippers({ { &arm, gripper_open_position_ } }) ||
        !detachObjectAtPose(arm, arm.config.target_position)) {
      return false;
    }

    setAllowedCollision(arm.config.object_id, table_id_, false);
    return executeStageWithSafetyReplan(
      arm, prefix + "retreat after place " + tag,
      [this, &arm, &prefix, &tag](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
        return planLinearOffset(
          arm, 0.0, 0.0, retreat_distance_, prefix + "retreat after place " + tag, plan);
      });
  }

  bool executeArmResumePickPlaceSequence(
    ArmRuntime& arm, const std::vector<double>& pick_position, double& selected_pick_yaw,
    const std::string& tag)
  {
    if (!executeStageWithSafetyReplan(
          arm, arm.config.label + " move to pregrasp " + tag,
          [this, &arm, &pick_position, &selected_pick_yaw](
            moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planPickPregrasp(arm, pick_position, selected_pick_yaw, plan);
          })) {
      return false;
    }

    return executeArmPostPregraspSequence(arm, tag);
  }

  bool executeRightArmResumePickPlaceWithFallback(
    const std::vector<double>& pick_position, double& selected_pick_yaw, const std::string& tag)
  {
    if (right_resume_planning_delay_sec_ > 1e-3) {
      RCLCPP_INFO(
        get_logger(),
        "Waiting %.2fs before replanning right arm %s from the waiting pose.",
        right_resume_planning_delay_sec_, tag.c_str());
      std::this_thread::sleep_for(std::chrono::duration<double>(right_resume_planning_delay_sec_));
    }

    if (executeArmResumePickPlaceSequence(right_arm_, pick_position, selected_pick_yaw, tag)) {
      return true;
    }

    if (!right_replan_fallback_to_home_) {
      return false;
    }

    RCLCPP_WARN(
      get_logger(),
      "Right arm could not resume %s directly from the waiting pose. Falling back to right home before one final retry.",
      tag.c_str());
    if (!moveArmToNamedTarget(
          right_arm_, right_arm_.config.home_pose_name, "right safety fallback home before " + tag)) {
      return false;
    }

    return executeArmResumePickPlaceSequence(
      right_arm_, pick_position, selected_pick_yaw, tag + " after home");
  }

  bool executeSingleArmPickPlace(ArmRuntime& arm)
  {
    if (!commandGrippers({ { &arm, gripper_open_position_ } })) {
      return false;
    }

    if (!ensureObjectPresentInWorld(arm.config.object_id, arm.config.source_position)) {
      RCLCPP_ERROR(get_logger(), "Failed to ensure object %s exists before %s pick.",
                   arm.config.object_id.c_str(), arm.config.label.c_str());
      return false;
    }
    std::this_thread::sleep_for(300ms);

    const auto pick_position = addOffset(arm.config.source_position, arm.config.source_position_offset);
    double selected_pick_yaw = 0.0;
    if (!executeStageWithSafetyReplan(
          arm, arm.config.label + " move to pregrasp sequential",
          [&arm, &pick_position, &selected_pick_yaw, this](
            moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planPickPregrasp(arm, pick_position, selected_pick_yaw, plan);
          })) {
      return false;
    }

    if (!executeStageWithSafetyReplan(
          arm, arm.config.label + " move to grasp sequential",
          [this, &arm](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planLinearOffset(
              arm, 0.0, 0.0, -pregrasp_offset_, arm.config.label + " move to grasp sequential", plan);
          }) ||
        !commandGrippers({ { &arm, gripper_closed_position_ } }) ||
        !attachObjectAtPose(arm, arm.config.source_position)) {
      return false;
    }

    setAllowedCollision(arm.config.object_id, table_id_, true);

    if (!executeStageWithSafetyReplan(
          arm, arm.config.label + " lift object sequential",
          [this, &arm](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planLinearOffset(
              arm, 0.0, 0.0, lift_distance_, arm.config.label + " lift object sequential", plan);
          })) {
      return false;
    }

    if (!executeStageWithSafetyReplan(
          arm, arm.config.label + " move to pre-place sequential",
          [&arm, this](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planPlacePreplace(arm, plan);
          }) ||
        !executeStageWithSafetyReplan(
          arm, arm.config.label + " lower to place sequential",
          [this, &arm](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
            return planLinearOffset(
              arm, 0.0, 0.0, -pregrasp_offset_, arm.config.label + " lower to place sequential", plan);
          }) ||
        !commandGrippers({ { &arm, gripper_open_position_ } }) ||
        !detachObjectAtPose(arm, arm.config.target_position)) {
      return false;
    }

    setAllowedCollision(arm.config.object_id, table_id_, false);
    return executeStageWithSafetyReplan(
      arm, arm.config.label + " retreat after place sequential",
      [this, &arm](moveit::planning_interface::MoveGroupInterface::Plan& plan) {
        return planLinearOffset(
          arm, 0.0, 0.0, retreat_distance_, arm.config.label + " retreat after place sequential", plan);
      });
  }

  bool moveArmToNamedTarget(ArmRuntime& arm, const std::string& named_target, const std::string& stage_name)
  {
    moveit::planning_interface::MoveGroupInterface::Plan plan;
    if (!planToNamedTarget(arm, named_target, plan)) {
      return false;
    }
    return executePlannedStages({ { &arm, plan, stage_name } });
  }

  bool planPlacePreplace(
    ArmRuntime& arm, moveit::planning_interface::MoveGroupInterface::Plan& preplace_plan)
  {
    const auto& yaw_candidates = arm.config.place_yaw_candidates.empty()
                                   ? std::vector<double>{ arm.config.place_yaw }
                                   : arm.config.place_yaw_candidates;
    for (const double yaw : yaw_candidates) {
      const auto preplace_pose = makeToolPose(
        arm.config.target_position[0], arm.config.target_position[1],
        arm.config.target_position[2] + grasp_height_offset_ + pregrasp_offset_, yaw);
      if (planToPose(
            arm, preplace_pose, arm.config.label + " move to pre-place sequential", preplace_plan)) {
        return true;
      }
    }

    RCLCPP_ERROR(
      get_logger(), "All %s place yaw candidates failed.", arm.config.label.c_str());
    return false;
  }

  bool executeStageWithSafetyReplan(
    ArmRuntime& arm, const std::string& stage_name,
    const std::function<bool(moveit::planning_interface::MoveGroupInterface::Plan&)>& planner)
  {
    const bool allow_risk_replan = enable_monitor_risk_replan_ && arm.config.label == right_arm_.config.label;
    const int max_attempts = allow_risk_replan ? std::max(1, max_right_arm_replan_attempts_ + 1) : 1;
    bool prefer_warn_slowdown = true;
    bool fallback_home_used = false;
    RiskAction last_interruption_action = RiskAction::NONE;
    for (int attempt = 1; attempt <= max_attempts; ++attempt) {
      moveit::planning_interface::MoveGroupInterface::Plan plan;
      if (!planner(plan)) {
        if (!allow_risk_replan || last_interruption_action == RiskAction::NONE || attempt >= max_attempts) {
          return false;
        }

        RCLCPP_WARN(
          get_logger(),
          "Replanning failed during safety recovery for stage '%s'. last_action=%s attempt %d/%d.",
          stage_name.c_str(), riskActionToString(last_interruption_action).c_str(), attempt, max_attempts);

        if ((last_interruption_action == RiskAction::STOP || last_interruption_action == RiskAction::REPLAN) &&
            right_replan_fallback_to_home_ && !fallback_home_used) {
          if (!moveArmToNamedTarget(
                right_arm_, right_arm_.config.home_pose_name, "right safety fallback home before replan")) {
            return false;
          }
          fallback_home_used = true;
        }
        continue;
      }

      if (allow_risk_replan && !prefer_warn_slowdown) {
        applyTrajectorySpeedScaling(plan, warn_velocity_scale_, warn_acceleration_scale_);
      }

      const auto results = executePlannedStagesDetailed({
        {
          &arm,
          plan,
          stage_name,
          0.0,
          allow_risk_replan,
          prefer_warn_slowdown,
        },
      });
      if (results.empty()) {
        return false;
      }

      const auto& result = results.front();
      if (result.succeeded) {
        return true;
      }

      if (result.interruption_action == RiskAction::NONE) {
        return false;
      }

      RCLCPP_WARN(
        get_logger(),
        "Risk-triggered interruption during stage '%s'. action=%s attempt %d/%d.",
        stage_name.c_str(), riskActionToString(result.interruption_action).c_str(), attempt, max_attempts);
      last_interruption_action = result.interruption_action;

      if (result.interruption_action == RiskAction::SLOWDOWN) {
        prefer_warn_slowdown = false;
        continue;
      }

      if ((result.interruption_action == RiskAction::STOP || result.interruption_action == RiskAction::REPLAN) &&
          allow_risk_replan &&
          right_replan_fallback_to_home_ && !fallback_home_used && attempt < max_attempts) {
        if (!moveArmToNamedTarget(
              right_arm_, right_arm_.config.home_pose_name, "right safety fallback home before replan")) {
          return false;
        }
        fallback_home_used = true;
      }
    }

    RCLCPP_ERROR(
      get_logger(), "Exceeded right-arm safety replan attempts for stage '%s'.", stage_name.c_str());
    return false;
  }

  static std::string riskActionToString(RiskAction action)
  {
    switch (action) {
      case RiskAction::SLOWDOWN:
        return "slowdown";
      case RiskAction::STOP:
        return "stop";
      case RiskAction::REPLAN:
        return "replan";
      case RiskAction::NONE:
      default:
        return "none";
    }
  }

  RiskAction recommendedActionForRisk(const std::string& risk) const
  {
    if (risk == "warn") {
      return RiskAction::SLOWDOWN;
    }
    if (risk == "stop") {
      return RiskAction::STOP;
    }
    if (risk == "collision") {
      return RiskAction::REPLAN;
    }
    return RiskAction::NONE;
  }

  RiskAction riskActionFromMonitorState(const MonitorRiskState& state) const
  {
    if (state.action == "slowdown") {
      return RiskAction::SLOWDOWN;
    }
    if (state.action == "stop") {
      return RiskAction::STOP;
    }
    if (state.action == "replan") {
      return RiskAction::REPLAN;
    }
    return recommendedActionForRisk(state.risk);
  }

  bool moveRightArmToLocalWait(const std::string& blocked_stage_name)
  {
    if (right_local_wait_planning_delay_sec_ > 1e-3) {
      RCLCPP_INFO(
        get_logger(),
        "Waiting %.2fs for state settling before planning right local wait for stage '%s'.",
        right_local_wait_planning_delay_sec_, blocked_stage_name.c_str());
      std::this_thread::sleep_for(
        std::chrono::duration<double>(right_local_wait_planning_delay_sec_));
    }

    if (!right_local_wait_pose_name_.empty()) {
      RCLCPP_INFO(
        get_logger(),
        "Trying dedicated named wait pose '%s' before stage '%s'.", right_local_wait_pose_name_.c_str(),
        blocked_stage_name.c_str());
      if (moveArmToNamedTarget(
            right_arm_, right_local_wait_pose_name_, "right move to named local wait before " + blocked_stage_name)) {
        return true;
      }

      RCLCPP_WARN(
        get_logger(),
        "Right arm failed to reach named local wait pose '%s' before stage '%s'. Will try geometric local-wait recovery.",
        right_local_wait_pose_name_.c_str(), blocked_stage_name.c_str());
    }

    bool elevated_wait_reached = false;
    if (right_local_wait_lift_distance_ > 1e-4) {
      moveit::planning_interface::MoveGroupInterface::Plan lift_plan;
      const std::string lift_stage_name = "right local wait lift before " + blocked_stage_name;
      if (planLinearOffset(right_arm_, 0.0, 0.0, right_local_wait_lift_distance_, lift_stage_name, lift_plan) &&
          executePlannedStages({ { &right_arm_, lift_plan, lift_stage_name } })) {
        elevated_wait_reached = true;
      } else {
        RCLCPP_WARN(
          get_logger(),
          "Right arm failed to reach elevated temporary wait before stage '%s'. Will try direct local-wait planning.",
          blocked_stage_name.c_str());
      }
    }

    if (hasMeaningfulOffset(right_local_wait_position_offset_)) {
      moveit::planning_interface::MoveGroupInterface::Plan wait_plan;
      const std::string wait_stage_name = "right move to local wait before " + blocked_stage_name;
      if (planToCurrentPoseOffset(right_arm_, right_local_wait_position_offset_, wait_stage_name, wait_plan) &&
          executePlannedStages({ { &right_arm_, wait_plan, wait_stage_name } })) {
        return true;
      }

      RCLCPP_WARN(
        get_logger(),
        "Right arm failed to shift into configured local wait pose before stage '%s'.",
        blocked_stage_name.c_str());
    }

    if (elevated_wait_reached) {
      RCLCPP_INFO(
        get_logger(),
        "Right arm will hold the elevated temporary wait pose before resuming stage '%s'.",
        blocked_stage_name.c_str());
      return true;
    }

    return false;
  }

  void handleMonitorRiskState(const std_msgs::msg::String::SharedPtr msg)
  {
    MonitorRiskState parsed_state;
    parsed_state.received_at = now();

    std::vector<std::string> fields;
    std::size_t start = 0;
    while (start <= msg->data.size()) {
      const auto separator = msg->data.find('|', start);
      if (separator == std::string::npos) {
        fields.push_back(msg->data.substr(start));
        break;
      }
      fields.push_back(msg->data.substr(start, separator - start));
      start = separator + 1;
    }

    if (!fields.empty()) {
      parsed_state.risk = fields[0];
    }

    const bool has_explicit_action =
      fields.size() > 1U &&
      (fields[1] == "none" || fields[1] == "slowdown" || fields[1] == "stop" || fields[1] == "replan");

    if (has_explicit_action) {
      parsed_state.action = fields[1];
    } else {
      parsed_state.action = riskActionToString(recommendedActionForRisk(parsed_state.risk));
    }

    if (fields.size() > (has_explicit_action ? 2U : 1U)) {
      parsed_state.scope = fields[has_explicit_action ? 2U : 1U];
    }
    if (fields.size() > (has_explicit_action ? 3U : 2U)) {
      parsed_state.first_body = fields[has_explicit_action ? 3U : 2U];
    }
    if (fields.size() > (has_explicit_action ? 4U : 3U)) {
      parsed_state.second_body = fields[has_explicit_action ? 4U : 3U];
    }
    if (fields.size() > (has_explicit_action ? 5U : 4U)) {
      try {
        parsed_state.future_offset = std::stod(fields[has_explicit_action ? 5U : 4U]);
      } catch (const std::exception&) {
        parsed_state.future_offset = 0.0;
      }
    }

    std::lock_guard<std::mutex> lock(monitor_risk_mutex_);
    latest_monitor_risk_ = parsed_state;
  }

  bool getLatestMonitorRiskState(MonitorRiskState& state) const
  {
    std::lock_guard<std::mutex> lock(monitor_risk_mutex_);
    if (!latest_monitor_risk_.has_value()) {
      return false;
    }
    state = *latest_monitor_risk_;
    return true;
  }

  bool isMonitorRiskUsable(const MonitorRiskState& current_risk) const
  {
    const auto timeout = rclcpp::Duration::from_seconds(std::max(0.0, monitor_risk_timeout_));
    return timeout.nanoseconds() <= 0 || (now() - current_risk.received_at) <= timeout;
  }

  bool shouldApplyWarnSlowdownBeforeExecution() const
  {
    MonitorRiskState current_risk;
    if (!getLatestMonitorRiskState(current_risk)) {
      return false;
    }
    if (current_risk.scope != "inter_arm") {
      return false;
    }
    if (!isMonitorRiskUsable(current_risk)) {
      return false;
    }
    return riskActionFromMonitorState(current_risk) == RiskAction::SLOWDOWN;
  }

  RiskAction requestedRiskActionForRunningStage(
    const PlannedStage& stage, const std::shared_ptr<RunningStageState>& running_stage) const
  {
    if (!stage.allow_risk_response || stage.arm != &right_arm_) {
      return RiskAction::NONE;
    }

    MonitorRiskState current_risk;
    if (!getLatestMonitorRiskState(current_risk) || current_risk.scope != "inter_arm" ||
        !isMonitorRiskUsable(current_risk)) {
      return RiskAction::NONE;
    }

    const RiskAction requested_action = riskActionFromMonitorState(current_risk);
    if (requested_action == RiskAction::NONE) {
      return RiskAction::NONE;
    }

    std::lock_guard<std::mutex> lock(running_stage->mutex);
    if (!running_stage->started || current_risk.received_at < running_stage->execution_start_time) {
      return RiskAction::NONE;
    }
    if (requested_action == RiskAction::SLOWDOWN &&
        (!stage.allow_warn_slowdown || running_stage->slowdown_applied)) {
      return RiskAction::NONE;
    }
    return requested_action;
  }

  bool worldObjectExists(const std::string& object_id)
  {
    const auto objects = planning_scene_interface_.getObjects({ object_id });
    return objects.find(object_id) != objects.end();
  }

  bool attachedObjectExists(const std::string& object_id)
  {
    const auto attached_objects = planning_scene_interface_.getAttachedObjects({ object_id });
    return attached_objects.find(object_id) != attached_objects.end();
  }

  bool ensureObjectPresentInWorld(const std::string& object_id, const std::vector<double>& world_position)
  {
    if (attachedObjectExists(object_id) || worldObjectExists(object_id)) {
      return true;
    }

    auto object = makeBoxObject(object_id, world_position, object_size_);
    std::lock_guard<std::mutex> lock(planning_scene_mutex_);
    if (!planning_scene_interface_.applyCollisionObject(object)) {
      return false;
    }

    std::this_thread::sleep_for(200ms);
    return true;
  }

  bool removeWorldObjectIfPresent(const std::string& object_id)
  {
    if (!worldObjectExists(object_id)) {
      return true;
    }

    std::lock_guard<std::mutex> lock(planning_scene_mutex_);
    planning_scene_interface_.removeCollisionObjects({ object_id });
    std::this_thread::sleep_for(200ms);
    return true;
  }

  std::vector<double> addOffset(const std::vector<double>& base, const std::vector<double>& offset) const
  {
    return {
      base[0] + offset[0],
      base[1] + offset[1],
      base[2] + offset[2],
    };
  }

  bool hasMeaningfulOffset(const std::vector<double>& offset) const
  {
    return offset.size() == 3 &&
           (std::abs(offset[0]) > 1e-6 || std::abs(offset[1]) > 1e-6 || std::abs(offset[2]) > 1e-6);
  }

  moveit_msgs::msg::CollisionObject makeBoxObject(
    const std::string& id, const std::vector<double>& position, const std::vector<double>& size) const
  {
    moveit_msgs::msg::CollisionObject collision_object;
    collision_object.id = id;
    collision_object.header.frame_id = world_frame_;

    shape_msgs::msg::SolidPrimitive primitive;
    primitive.type = primitive.BOX;
    primitive.dimensions.insert(primitive.dimensions.end(), size.begin(), size.end());

    geometry_msgs::msg::Pose pose;
    pose.position.x = position[0];
    pose.position.y = position[1];
    pose.position.z = position[2];
    pose.orientation.w = 1.0;

    collision_object.primitives.push_back(primitive);
    collision_object.primitive_poses.push_back(pose);
    collision_object.operation = collision_object.ADD;
    return collision_object;
  }

  geometry_msgs::msg::PoseStamped makeToolPose(double x, double y, double z, double yaw) const
  {
    tf2::Quaternion orientation;
    orientation.setRPY(0.0, M_PI, yaw);
    orientation.normalize();

    geometry_msgs::msg::PoseStamped pose;
    pose.header.frame_id = world_frame_;
    pose.pose.position.x = x;
    pose.pose.position.y = y;
    pose.pose.position.z = z;
    pose.pose.orientation.x = orientation.x();
    pose.pose.orientation.y = orientation.y();
    pose.pose.orientation.z = orientation.z();
    pose.pose.orientation.w = orientation.w();
    return pose;
  }

  bool planPickPregrasp(
    ArmRuntime& arm, const std::vector<double>& adjusted_source_position, double& selected_pick_yaw,
    moveit::planning_interface::MoveGroupInterface::Plan& pregrasp_plan)
  {
    for (const double yaw : arm.config.pick_yaw_candidates) {
      const auto pick_pregrasp_pose = makeToolPose(
        adjusted_source_position[0], adjusted_source_position[1],
        adjusted_source_position[2] + grasp_height_offset_ + pregrasp_offset_, yaw);
      if (planToPose(arm, pick_pregrasp_pose, arm.config.label + " move to pregrasp", pregrasp_plan)) {
        selected_pick_yaw = yaw;
        return true;
      }
    }

    RCLCPP_ERROR(get_logger(), "All %s pick yaw candidates failed.", arm.config.label.c_str());
    return false;
  }

  bool planToPose(
    ArmRuntime& arm, const geometry_msgs::msg::PoseStamped& pose, const std::string& stage_name,
    moveit::planning_interface::MoveGroupInterface::Plan& plan)
  {
    arm.move_group->setStartStateToCurrentState();
    arm.move_group->setPoseTarget(pose, arm.config.hand_frame);

    const bool planned = arm.move_group->plan(plan) == moveit::core::MoveItErrorCode::SUCCESS;
    arm.move_group->clearPoseTargets();
    if (!planned) {
      RCLCPP_ERROR(get_logger(), "Planning failed during stage: %s", stage_name.c_str());
      return false;
    }
    return true;
  }

  bool planToCurrentPoseOffset(
    ArmRuntime& arm, const std::vector<double>& offset, const std::string& stage_name,
    moveit::planning_interface::MoveGroupInterface::Plan& plan)
  {
    if (!hasPoseVector(offset)) {
      RCLCPP_ERROR(get_logger(), "Local wait offset must contain exactly 3 elements.");
      return false;
    }

    auto current_pose = arm.move_group->getCurrentPose(arm.config.hand_frame);
    current_pose.header.frame_id = world_frame_;
    current_pose.pose.position.x += offset[0];
    current_pose.pose.position.y += offset[1];
    current_pose.pose.position.z += offset[2];
    return planToPose(arm, current_pose, stage_name, plan);
  }

  bool planLinearOffset(
    ArmRuntime& arm, double dx, double dy, double dz, const std::string& stage_name,
    moveit::planning_interface::MoveGroupInterface::Plan& plan)
  {
    arm.move_group->setStartStateToCurrentState();

    auto current_pose = arm.move_group->getCurrentPose(arm.config.hand_frame);
    geometry_msgs::msg::Pose target_pose = current_pose.pose;
    target_pose.position.x += dx;
    target_pose.position.y += dy;
    target_pose.position.z += dz;

    std::vector<geometry_msgs::msg::Pose> waypoints = { target_pose };
    moveit_msgs::msg::RobotTrajectory trajectory;
    const double fraction = arm.move_group->computeCartesianPath(
      waypoints, cartesian_eef_step_, cartesian_jump_threshold_, trajectory, true);
    if (fraction < cartesian_min_fraction_) {
      RCLCPP_WARN(
        get_logger(),
        "Cartesian path fell below threshold during stage: %s (fraction=%.3f). Falling back to sampled planning.",
        stage_name.c_str(), fraction);

      geometry_msgs::msg::PoseStamped target_pose_stamped;
      target_pose_stamped.header.frame_id = world_frame_;
      target_pose_stamped.pose = target_pose;
      return planToPose(arm, target_pose_stamped, stage_name + " fallback pose plan", plan);
    }

    plan.trajectory_ = trajectory;
    return true;
  }

  bool planToNamedTarget(
    ArmRuntime& arm, const std::string& named_target, moveit::planning_interface::MoveGroupInterface::Plan& plan)
  {
    for (int attempt = 1; attempt <= std::max(1, max_named_target_retries_); ++attempt) {
      arm.move_group->setStartStateToCurrentState();
      arm.move_group->setNamedTarget(named_target);
      if (arm.move_group->plan(plan) == moveit::core::MoveItErrorCode::SUCCESS) {
        return true;
      }

      RCLCPP_WARN(
        get_logger(), "Planning failed for named target: %s (attempt %d/%d)", named_target.c_str(), attempt,
        std::max(1, max_named_target_retries_));
    }
    RCLCPP_ERROR(get_logger(), "Planning failed for named target after retries: %s", named_target.c_str());
    return false;
  }

  void applyTrajectorySpeedScaling(
    moveit::planning_interface::MoveGroupInterface::Plan& plan, double velocity_scale,
    double acceleration_scale) const
  {
    const double clamped_velocity_scale = std::clamp(velocity_scale, 0.05, 1.0);
    const double clamped_acceleration_scale = std::clamp(acceleration_scale, 0.05, 1.0);
    const double time_scale = 1.0 / clamped_velocity_scale;

    for (auto& point : plan.trajectory_.joint_trajectory.points) {
      const double scaled_time = rclcpp::Duration(point.time_from_start).seconds() * time_scale;
      point.time_from_start = rclcpp::Duration::from_seconds(scaled_time);
      for (auto& velocity : point.velocities) {
        velocity *= clamped_velocity_scale;
      }
      for (auto& acceleration : point.accelerations) {
        acceleration *= clamped_acceleration_scale;
      }
    }

    for (auto& point : plan.trajectory_.multi_dof_joint_trajectory.points) {
      const double scaled_time = rclcpp::Duration(point.time_from_start).seconds() * time_scale;
      point.time_from_start = rclcpp::Duration::from_seconds(scaled_time);
    }
  }

  std::vector<StageExecutionResult> executePlannedStagesDetailed(const std::vector<PlannedStage>& stages)
  {
    if (stages.empty()) {
      return {};
    }

    for (const auto& stage : stages) {
      publishMonitoredTrajectory(*stage.arm, stage.plan, stage.stage_name, stage.start_delay_sec);
    }

    const double lead_time = std::max(0.0, monitor_trajectory_publish_lead_time_);
    if (lead_time > 0.0) {
      std::this_thread::sleep_for(std::chrono::duration<double>(lead_time));
    }

    std::vector<std::future<bool>> futures;
    std::vector<bool> stop_requested(stages.size(), false);
    std::vector<bool> completed(stages.size(), false);
    std::vector<std::shared_ptr<RunningStageState>> running_states;
    std::vector<StageExecutionResult> results(stages.size());
    futures.reserve(stages.size());
    running_states.reserve(stages.size());
    for (const auto& stage : stages) {
      auto running_state = std::make_shared<RunningStageState>();
      running_states.push_back(running_state);

      ArmRuntime* arm = stage.arm;
      auto plan = stage.plan;
      futures.push_back(std::async(std::launch::async, [this, arm, plan, stage, running_state]() mutable {
        if (stage.start_delay_sec > 0.0) {
          std::this_thread::sleep_for(std::chrono::duration<double>(stage.start_delay_sec));
        }

        if (stage.allow_risk_response && arm == &right_arm_ && shouldApplyWarnSlowdownBeforeExecution()) {
          applyTrajectorySpeedScaling(plan, warn_velocity_scale_, warn_acceleration_scale_);
          std::lock_guard<std::mutex> state_lock(running_state->mutex);
          running_state->slowdown_applied = true;
        }

        {
          std::lock_guard<std::mutex> state_lock(running_state->mutex);
          running_state->started = true;
          running_state->execution_start_time = now();
        }

        return arm->move_group->execute(plan) == moveit::core::MoveItErrorCode::SUCCESS;
      }));
    }

    std::size_t completed_count = 0;
    while (completed_count < stages.size()) {
      for (std::size_t index = 0; index < stages.size(); ++index) {
        if (completed[index]) {
          continue;
        }

        if (!stop_requested[index]) {
          const RiskAction requested_action =
            requestedRiskActionForRunningStage(stages[index], running_states[index]);
          if (requested_action != RiskAction::NONE) {
            RCLCPP_WARN(
              get_logger(), "Stopping right arm due to monitor action=%s during stage: %s",
              riskActionToString(requested_action).c_str(), stages[index].stage_name.c_str());
            right_arm_.move_group->stop();
            stop_requested[index] = true;
            results[index].interruption_action = requested_action;
          }
        }

        if (futures[index].wait_for(0ms) != std::future_status::ready) {
          continue;
        }

        const bool succeeded = futures[index].get();
        {
          std::lock_guard<std::mutex> state_lock(running_states[index]->mutex);
          results[index].slowdown_applied = running_states[index]->slowdown_applied;
        }
        if (stop_requested[index]) {
          results[index].succeeded = false;
        } else if (!succeeded) {
          RCLCPP_ERROR(
            get_logger(), "Execution failed during stage: %s", stages[index].stage_name.c_str());
          results[index].succeeded = false;
        } else {
          results[index].succeeded = true;
        }
        completed[index] = true;
        ++completed_count;
      }

      if (completed_count < stages.size()) {
        std::this_thread::sleep_for(100ms);
      }
    }

    std::this_thread::sleep_for(300ms);
    return results;
  }

  bool executePlannedStages(const std::vector<PlannedStage>& stages)
  {
    const auto results = executePlannedStagesDetailed(stages);
    if (results.size() != stages.size()) {
      return false;
    }

    for (const auto& result : results) {
      if (!result.succeeded) {
        return false;
      }
    }
    return true;
  }

  void publishMonitoredTrajectory(
    const ArmRuntime& arm, const moveit::planning_interface::MoveGroupInterface::Plan& plan,
    const std::string& stage_name, double start_delay_sec = 0.0)
  {
    if (!enable_fcl_monitor_publisher_ || !monitored_trajectory_publisher_) {
      return;
    }

    moveit_msgs::msg::DisplayTrajectory display_trajectory;
    display_trajectory.model_id = arm.config.arm_group;
    display_trajectory.trajectory.push_back(plan.trajectory_);

    const auto current_state = arm.move_group->getCurrentState(1.0);
    if (current_state) {
      moveit::core::robotStateToRobotStateMsg(*current_state, display_trajectory.trajectory_start);
    } else {
      RCLCPP_WARN(
        get_logger(), "Failed to fetch current state before stage '%s'; monitor will fall back to scene state.",
        stage_name.c_str());
    }

    const auto execution_start_time = now() + rclcpp::Duration::from_seconds(
      std::max(0.0, monitor_trajectory_publish_lead_time_ + std::max(0.0, start_delay_sec)));
    display_trajectory.trajectory.front().joint_trajectory.header.stamp = execution_start_time;
    monitored_trajectory_publisher_->publish(display_trajectory);

    RCLCPP_INFO(
      get_logger(), "Published monitored trajectory for stage '%s' on %s.", stage_name.c_str(),
      monitor_trajectory_topic_.c_str());
  }

  bool commandGrippers(const std::vector<std::pair<ArmRuntime*, double>>& commands)
  {
    std::vector<std::future<bool>> futures;
    futures.reserve(commands.size());
    for (const auto& [arm, position] : commands) {
      futures.push_back(std::async(std::launch::async, [this, arm, position]() {
        return commandGripper(*arm, position);
      }));
    }

    bool all_succeeded = true;
    for (auto& future : futures) {
      all_succeeded = future.get() && all_succeeded;
    }
    std::this_thread::sleep_for(300ms);
    return all_succeeded;
  }

  bool commandGripper(ArmRuntime& arm, double position)
  {
    if (!arm.gripper_action_client->wait_for_action_server(10s)) {
      RCLCPP_ERROR(
        get_logger(), "%s gripper action server is not available on %s.",
        arm.config.label.c_str(), arm.config.gripper_action_name.c_str());
      return false;
    }

    GripperCommand::Goal goal;
    goal.command.position = position;
    goal.command.max_effort = -1.0;

    auto goal_future = arm.gripper_action_client->async_send_goal(goal);
    if (goal_future.wait_for(10s) != std::future_status::ready) {
      RCLCPP_ERROR(get_logger(), "Timed out while sending %s gripper goal.", arm.config.label.c_str());
      return false;
    }

    const auto goal_handle = goal_future.get();
    if (!goal_handle) {
      RCLCPP_ERROR(get_logger(), "%s gripper goal was rejected.", arm.config.label.c_str());
      return false;
    }

    auto result_future = arm.gripper_action_client->async_get_result(goal_handle);
    if (result_future.wait_for(15s) != std::future_status::ready) {
      RCLCPP_ERROR(get_logger(), "Timed out while waiting for %s gripper result.", arm.config.label.c_str());
      return false;
    }

    const auto result = result_future.get();
    if (result.code != rclcpp_action::ResultCode::SUCCEEDED) {
      RCLCPP_ERROR(get_logger(), "%s gripper action did not succeed.", arm.config.label.c_str());
      return false;
    }

    return true;
  }

  bool attachObjectAtPose(ArmRuntime& arm, const std::vector<double>& world_position)
  {
    if (attachedObjectExists(arm.config.object_id)) {
      return true;
    }

    const auto* hand_jmg = arm.move_group->getRobotModel()->getJointModelGroup(arm.config.hand_group);
    const auto touch_links =
      hand_jmg ? hand_jmg->getLinkModelNamesWithCollisionGeometry() : std::vector<std::string>{};

    moveit_msgs::msg::AttachedCollisionObject attached_object;
    attached_object.link_name = arm.config.hand_frame;
    attached_object.touch_links = touch_links;
    attached_object.object = makeBoxObject(arm.config.object_id, world_position, object_size_);

    std::lock_guard<std::mutex> lock(planning_scene_mutex_);
    if (!planning_scene_interface_.applyAttachedCollisionObject(attached_object)) {
      RCLCPP_ERROR(get_logger(), "Failed to attach object %s to %s arm.", arm.config.object_id.c_str(),
                   arm.config.label.c_str());
      return false;
    }

    std::this_thread::sleep_for(300ms);
    return true;
  }

  bool detachObjectAtPose(ArmRuntime& arm, const std::vector<double>& world_position)
  {
    if (!attachedObjectExists(arm.config.object_id)) {
      return ensureObjectPresentInWorld(arm.config.object_id, world_position);
    }

    moveit_msgs::msg::AttachedCollisionObject attached_object;
    attached_object.link_name = arm.config.hand_frame;
    attached_object.object.id = arm.config.object_id;
    attached_object.object.operation = moveit_msgs::msg::CollisionObject::REMOVE;
    {
      std::lock_guard<std::mutex> lock(planning_scene_mutex_);
      if (!planning_scene_interface_.applyAttachedCollisionObject(attached_object)) {
        RCLCPP_ERROR(get_logger(), "Failed to detach object %s from %s arm.", arm.config.object_id.c_str(),
                     arm.config.label.c_str());
        return false;
      }
    }

    std::this_thread::sleep_for(300ms);

    if (!ensureObjectPresentInWorld(arm.config.object_id, world_position)) {
      RCLCPP_ERROR(get_logger(), "Failed to place detached object %s back into planning scene.",
                   arm.config.object_id.c_str());
      return false;
    }

    std::this_thread::sleep_for(300ms);
    return true;
  }

  void setAllowedCollision(const std::string& first, const std::string& second, bool allowed)
  {
    std::lock_guard<std::mutex> lock(planning_scene_mutex_);
    if (!planning_scene_client_->wait_for_service(5s)) {
      RCLCPP_WARN(get_logger(), "GetPlanningScene service unavailable, skip ACM update.");
      return;
    }

    auto request = std::make_shared<moveit_msgs::srv::GetPlanningScene::Request>();
    request->components.components =
      moveit_msgs::msg::PlanningSceneComponents::ALLOWED_COLLISION_MATRIX;

    auto response_future = planning_scene_client_->async_send_request(request);
    if (response_future.wait_for(5s) != std::future_status::ready) {
      RCLCPP_WARN(get_logger(), "Timed out while fetching planning scene ACM.");
      return;
    }

    auto planning_scene = response_future.get()->scene;
    planning_scene.is_diff = true;

    auto& acm = planning_scene.allowed_collision_matrix;

    auto ensure_entry = [&acm](const std::string& name) {
      auto it = std::find(acm.entry_names.begin(), acm.entry_names.end(), name);
      if (it != acm.entry_names.end()) {
        return static_cast<std::size_t>(std::distance(acm.entry_names.begin(), it));
      }

      acm.entry_names.push_back(name);
      for (auto& entry : acm.entry_values) {
        entry.enabled.push_back(false);
      }

      moveit_msgs::msg::AllowedCollisionEntry new_entry;
      new_entry.enabled.resize(acm.entry_names.size(), false);
      acm.entry_values.push_back(new_entry);
      return acm.entry_names.size() - 1;
    };

    updateAllowedCollisionEntry(acm, ensure_entry, first, second, allowed);

    planning_scene_interface_.applyPlanningScene(planning_scene);
    std::this_thread::sleep_for(200ms);
  }

  void allowLeftArmAgainstRightArm(bool allowed)
  {
    std::lock_guard<std::mutex> lock(planning_scene_mutex_);
    if (!planning_scene_client_->wait_for_service(5s)) {
      RCLCPP_WARN(get_logger(), "GetPlanningScene service unavailable, skip left/right ACM update.");
      return;
    }

    auto request = std::make_shared<moveit_msgs::srv::GetPlanningScene::Request>();
    request->components.components =
      moveit_msgs::msg::PlanningSceneComponents::ALLOWED_COLLISION_MATRIX;

    auto response_future = planning_scene_client_->async_send_request(request);
    if (response_future.wait_for(5s) != std::future_status::ready) {
      RCLCPP_WARN(get_logger(), "Timed out while fetching planning scene for left/right ACM update.");
      return;
    }

    auto planning_scene = response_future.get()->scene;
    planning_scene.is_diff = true;
    auto& acm = planning_scene.allowed_collision_matrix;

    auto ensure_entry = [&acm](const std::string& name) {
      auto it = std::find(acm.entry_names.begin(), acm.entry_names.end(), name);
      if (it != acm.entry_names.end()) {
        return static_cast<std::size_t>(std::distance(acm.entry_names.begin(), it));
      }

      acm.entry_names.push_back(name);
      for (auto& entry : acm.entry_values) {
        entry.enabled.push_back(false);
      }

      moveit_msgs::msg::AllowedCollisionEntry new_entry;
      new_entry.enabled.resize(acm.entry_names.size(), false);
      acm.entry_values.push_back(new_entry);
      return acm.entry_names.size() - 1;
    };

    const auto all_links =
      left_arm_.move_group->getRobotModel()->getLinkModelNamesWithCollisionGeometry();
    std::vector<std::string> left_links;
    std::vector<std::string> right_links;

    for (const auto& link : all_links) {
      if (link.rfind("right_", 0) == 0) {
        right_links.push_back(link);
      } else if (link != "world") {
        left_links.push_back(link);
      }
    }

    for (const auto& left_link : left_links) {
      for (const auto& right_link : right_links) {
        updateAllowedCollisionEntry(acm, ensure_entry, left_link, right_link, allowed);
      }
    }

    planning_scene_interface_.applyPlanningScene(planning_scene);
    std::this_thread::sleep_for(200ms);
  }

  template <typename EnsureEntryFn>
  void updateAllowedCollisionEntry(
    moveit_msgs::msg::AllowedCollisionMatrix& acm, EnsureEntryFn&& ensure_entry,
    const std::string& first, const std::string& second, bool allowed)
  {
    const std::size_t first_index = ensure_entry(first);
    const std::size_t second_index = ensure_entry(second);
    acm.entry_values[first_index].enabled[second_index] = allowed;
    acm.entry_values[second_index].enabled[first_index] = allowed;
  }

  moveit::planning_interface::PlanningSceneInterface planning_scene_interface_;
  rclcpp::Client<moveit_msgs::srv::GetPlanningScene>::SharedPtr planning_scene_client_;
  rclcpp::Publisher<moveit_msgs::msg::DisplayTrajectory>::SharedPtr monitored_trajectory_publisher_;
  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr monitor_risk_subscription_;

  mutable std::mutex planning_scene_mutex_;
  mutable std::mutex monitor_risk_mutex_;
  std::optional<MonitorRiskState> latest_monitor_risk_;

  ArmRuntime left_arm_;
  ArmRuntime right_arm_;
  std::string world_frame_;
  std::string table_id_;
  std::string monitor_trajectory_topic_;
  std::string monitor_risk_topic_;
  bool ignore_right_arm_collisions_{};
  bool enable_master_slave_sequencing_{};
  bool move_master_home_before_slave_resume_{};
  bool enable_fcl_monitor_publisher_{};
  bool enable_monitor_risk_replan_{};
  bool enable_right_local_wait_recovery_{};
  bool enable_staggered_dual_arm_execution_{};
  bool right_replan_fallback_to_home_{};
  double monitor_trajectory_publish_lead_time_{};
  double monitor_risk_timeout_{};
  double right_local_wait_lift_distance_{};
  double right_local_wait_planning_delay_sec_{};
  double right_resume_planning_delay_sec_{};
  double secondary_start_delay_{};
  double warn_velocity_scale_{};
  double warn_acceleration_scale_{};
  int max_right_arm_replan_attempts_{};
  int max_named_target_retries_{};
  std::string primary_start_arm_;
  std::string right_local_wait_pose_name_;
  std::vector<double> right_local_wait_position_offset_;
  std::vector<double> left_priority_clear_position_offset_;

  std::vector<double> object_size_;
  std::vector<double> table_position_;
  std::vector<double> table_size_;
  double grasp_height_offset_{};
  double pregrasp_offset_{};
  double lift_distance_{};
  double retreat_distance_{};
  double planning_time_{};
  int planning_attempts_{};
  double max_velocity_scaling_{};
  double max_acceleration_scaling_{};
  double gripper_open_position_{};
  double gripper_closed_position_{};
  bool return_home_after_place_{};
  std::string planner_id_;
  double cartesian_eef_step_{};
  double cartesian_jump_threshold_{};
  double cartesian_min_fraction_{};
  double workspace_min_x_{};
  double workspace_max_x_{};
  double workspace_min_y_{};
  double workspace_max_y_{};
  double workspace_min_z_{};
  double workspace_max_z_{};
};

int main(int argc, char** argv)
{
  rclcpp::init(argc, argv);

  auto node = std::make_shared<SimplePickPlaceNode>(rclcpp::NodeOptions{});
  node->init();

  rclcpp::executors::SingleThreadedExecutor executor;
  executor.add_node(node);
  std::thread spin_thread([&executor]() { executor.spin(); });

  const bool success = node->run();

  executor.cancel();
  spin_thread.join();
  rclcpp::shutdown();
  return success ? 0 : 1;
}
