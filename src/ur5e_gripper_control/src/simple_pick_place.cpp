#include <chrono>
#include <cmath>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <control_msgs/action/gripper_command.hpp>
#include <geometry_msgs/msg/pose_stamped.hpp>
#include <moveit/move_group_interface/move_group_interface.h>
#include <moveit/planning_scene_interface/planning_scene_interface.h>
#include <moveit_msgs/msg/allowed_collision_entry.hpp>
#include <moveit_msgs/msg/collision_object.hpp>
#include <moveit_msgs/msg/planning_scene_components.hpp>
#include <moveit_msgs/msg/planning_scene.hpp>
#include <moveit_msgs/srv/get_planning_scene.hpp>
#include <rclcpp/rclcpp.hpp>
#include <rclcpp_action/rclcpp_action.hpp>
#include <shape_msgs/msg/solid_primitive.hpp>
#include <tf2/LinearMath/Quaternion.h>

using namespace std::chrono_literals;

class SimplePickPlaceNode : public rclcpp::Node
{
  using GripperCommand = control_msgs::action::GripperCommand;
  using GoalHandleGripperCommand = rclcpp_action::ClientGoalHandle<GripperCommand>;

public:
  explicit SimplePickPlaceNode(const rclcpp::NodeOptions& options)
  : Node("simple_pick_place", options)
  {
    world_frame_ = declare_parameter<std::string>("world_frame", "world");
    object_id_ = declare_parameter<std::string>("object_id", "blue_box");
    table_id_ = declare_parameter<std::string>("table_id", "table");
    arm_group_ = declare_parameter<std::string>("arm_group", "ur_manipulator");
    hand_group_ = declare_parameter<std::string>("hand_group", "gripper");
    hand_frame_ = declare_parameter<std::string>("hand_frame", "tool0");
    home_pose_name_ = declare_parameter<std::string>("home_pose_name", "ready");
    right_arm_group_ = declare_parameter<std::string>("right_arm_group", "right_ur_manipulator");
    right_home_pose_name_ = declare_parameter<std::string>("right_home_pose_name", "right_ready");
    ignore_right_arm_collisions_ = declare_parameter<bool>("ignore_right_arm_collisions", true);

    source_position_ = declare_parameter<std::vector<double>>("source_position", { 0.1, 0.1, 1.04 });
    target_position_ = declare_parameter<std::vector<double>>("target_position", { -0.15, -0.25, 1.04 });
    object_size_ = declare_parameter<std::vector<double>>("object_size", { 0.05, 0.05, 0.05 });
    table_position_ = declare_parameter<std::vector<double>>("table_position", { 0.0, 0.0, 1.0 });
    table_size_ = declare_parameter<std::vector<double>>("table_size", { 1.8, 0.8, 0.03 });
    source_position_offset_ =
      declare_parameter<std::vector<double>>("source_position_offset", { -0.012, 0.004, 0.0 });

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
    return_home_after_place_ = declare_parameter<bool>("return_home_after_place", false);
    planner_id_ = declare_parameter<std::string>("planner_id", "RRTConnectkConfigDefault");
    cartesian_eef_step_ = declare_parameter<double>("cartesian_eef_step", 0.01);
    cartesian_jump_threshold_ = declare_parameter<double>("cartesian_jump_threshold", 0.0);
    cartesian_min_fraction_ = declare_parameter<double>("cartesian_min_fraction", 0.95);
    pick_yaw_candidates_ =
      declare_parameter<std::vector<double>>("pick_yaw_candidates", { 0.0, 1.57079632679, -1.57079632679 });
    place_yaw_ = declare_parameter<double>("place_yaw", 0.0);
    workspace_margin_xy_ = declare_parameter<double>("workspace_margin_xy", 0.05);
    workspace_min_z_ = declare_parameter<double>("workspace_min_z", 0.95);
    workspace_max_z_ = declare_parameter<double>("workspace_max_z", 1.45);

    gripper_action_client_ =
      rclcpp_action::create_client<GripperCommand>(this, "/gripper_controller/gripper_cmd");
    planning_scene_client_ =
      create_client<moveit_msgs::srv::GetPlanningScene>("/get_planning_scene");
  }

  void init()
  {
    move_group_ = std::make_shared<moveit::planning_interface::MoveGroupInterface>(shared_from_this(), arm_group_);
    right_move_group_ =
      std::make_shared<moveit::planning_interface::MoveGroupInterface>(shared_from_this(), right_arm_group_);

    move_group_->setPoseReferenceFrame(world_frame_);
    move_group_->setEndEffectorLink(hand_frame_);
    move_group_->setPlanningTime(planning_time_);
    move_group_->setNumPlanningAttempts(planning_attempts_);
    move_group_->setMaxVelocityScalingFactor(max_velocity_scaling_);
    move_group_->setMaxAccelerationScalingFactor(max_acceleration_scaling_);
    move_group_->setPlannerId(planner_id_);
    move_group_->setWorkspace(
      table_position_[0] - table_size_[0] * 0.5 - workspace_margin_xy_,
      table_position_[1] - table_size_[1] * 0.5 - workspace_margin_xy_,
      workspace_min_z_,
      table_position_[0] + table_size_[0] * 0.5 + workspace_margin_xy_,
      table_position_[1] + table_size_[1] * 0.5 + workspace_margin_xy_,
      workspace_max_z_);
    move_group_->allowReplanning(true);

    right_move_group_->setPlanningTime(planning_time_);
    right_move_group_->setNumPlanningAttempts(planning_attempts_);
    right_move_group_->setMaxVelocityScalingFactor(max_velocity_scaling_);
    right_move_group_->setMaxAccelerationScalingFactor(max_acceleration_scaling_);
    right_move_group_->setPlannerId(planner_id_);
    right_move_group_->allowReplanning(true);
  }

  bool run()
  {
    if (!validateParameters()) {
      return false;
    }

    if (!waitForCurrentState()) {
      return false;
    }

    if (!moveToNamedTarget(right_move_group_, right_home_pose_name_)) {
      RCLCPP_ERROR(get_logger(), "Failed to move right arm to safe standby pose.");
      return false;
    }

    if (ignore_right_arm_collisions_) {
      allowLeftArmAgainstRightArm(true);
    }

    setupPlanningScene();

    if (!commandGripper(gripper_open_position_)) {
      return false;
    }

    const std::vector<double> adjusted_source_position = {
      source_position_[0] + source_position_offset_[0],
      source_position_[1] + source_position_offset_[1],
      source_position_[2] + source_position_offset_[2],
    };

    double selected_pick_yaw = 0.0;
    if (!moveToPickSequence(adjusted_source_position, selected_pick_yaw)) {
      return false;
    }

    if (!commandGripper(gripper_closed_position_)) {
      return false;
    }

    if (!attachObjectAtPose(source_position_)) {
      return false;
    }

    setAllowedCollision(object_id_, table_id_, true);

    if (!moveLinearOffset(0.0, 0.0, lift_distance_, "lift object")) {
      return false;
    }

    const auto place_pregrasp = makeToolPose(
      target_position_[0], target_position_[1],
      target_position_[2] + grasp_height_offset_ + pregrasp_offset_, place_yaw_);

    if (!moveToPose(place_pregrasp, "move to pre-place")) {
      return false;
    }

    if (!moveLinearOffset(0.0, 0.0, -pregrasp_offset_, "lower to place")) {
      return false;
    }

    if (!commandGripper(gripper_open_position_)) {
      return false;
    }

    if (!detachObjectAtPose(target_position_)) {
      return false;
    }

    setAllowedCollision(object_id_, table_id_, false);

    if (!moveLinearOffset(0.0, 0.0, retreat_distance_, "retreat after place")) {
      return false;
    }

    if (return_home_after_place_ && !moveToNamedTarget(move_group_, home_pose_name_)) {
      return false;
    }

    RCLCPP_INFO(get_logger(), "Simple MoveIt2 pick and place finished successfully.");
    return true;
  }

private:
  bool validateParameters() const
  {
    return source_position_.size() == 3 && target_position_.size() == 3 && object_size_.size() == 3 &&
           table_position_.size() == 3 && table_size_.size() == 3 &&
           source_position_offset_.size() == 3;
  }

  bool waitForCurrentState()
  {
    for (int attempt = 0; attempt < 30; ++attempt) {
      if (move_group_->getCurrentState(1.0)) {
        return true;
      }
      RCLCPP_INFO_THROTTLE(
        get_logger(), *get_clock(), 2000, "Waiting for current robot state from MoveIt...");
    }
    RCLCPP_ERROR(get_logger(), "Failed to receive current robot state.");
    return false;
  }

  void setupPlanningScene()
  {
    std::vector<moveit_msgs::msg::CollisionObject> collision_objects;
    collision_objects.push_back(makeBoxObject(table_id_, table_position_, table_size_));
    collision_objects.push_back(makeBoxObject(object_id_, source_position_, object_size_));
    planning_scene_interface_.applyCollisionObjects(collision_objects);
    std::this_thread::sleep_for(500ms);
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

  bool moveToPickSequence(const std::vector<double>& adjusted_source_position, double& selected_pick_yaw)
  {
    for (const double yaw : pick_yaw_candidates_) {
      const auto pick_pregrasp = makeToolPose(
        adjusted_source_position[0], adjusted_source_position[1],
        adjusted_source_position[2] + grasp_height_offset_ + pregrasp_offset_, yaw);

      if (!moveToPose(pick_pregrasp, "move to pregrasp")) {
        continue;
      }

      // Remove the object for the final grasp approach so the tool can close around it.
      planning_scene_interface_.removeCollisionObjects({ object_id_ });
      std::this_thread::sleep_for(300ms);

      if (!moveLinearOffset(0.0, 0.0, -pregrasp_offset_, "move to grasp")) {
        setupPlanningScene();
        continue;
      }

      selected_pick_yaw = yaw;
      return true;
    }

    RCLCPP_ERROR(get_logger(), "All pick yaw candidates failed.");
    return false;
  }

  bool moveToPose(const geometry_msgs::msg::PoseStamped& pose, const std::string& stage_name)
  {
    move_group_->setStartStateToCurrentState();
    move_group_->setPoseTarget(pose, hand_frame_);

    moveit::planning_interface::MoveGroupInterface::Plan plan;
    const bool planned = move_group_->plan(plan) == moveit::core::MoveItErrorCode::SUCCESS;
    move_group_->clearPoseTargets();

    if (!planned) {
      RCLCPP_ERROR(get_logger(), "Planning failed during stage: %s", stage_name.c_str());
      return false;
    }

    const bool executed = move_group_->execute(plan) == moveit::core::MoveItErrorCode::SUCCESS;
    if (!executed) {
      RCLCPP_ERROR(get_logger(), "Execution failed during stage: %s", stage_name.c_str());
      return false;
    }

    std::this_thread::sleep_for(300ms);
    return true;
  }

  bool moveLinearOffset(double dx, double dy, double dz, const std::string& stage_name)
  {
    move_group_->setStartStateToCurrentState();

    auto current_pose = move_group_->getCurrentPose(hand_frame_);
    geometry_msgs::msg::Pose target_pose = current_pose.pose;
    target_pose.position.x += dx;
    target_pose.position.y += dy;
    target_pose.position.z += dz;

    std::vector<geometry_msgs::msg::Pose> waypoints = { target_pose };
    moveit_msgs::msg::RobotTrajectory trajectory;
    const double fraction = move_group_->computeCartesianPath(
      waypoints, cartesian_eef_step_, cartesian_jump_threshold_, trajectory, true);

    if (fraction < cartesian_min_fraction_) {
      RCLCPP_ERROR(
        get_logger(), "Cartesian path failed during stage: %s (fraction=%.3f)",
        stage_name.c_str(), fraction);
      return false;
    }

    moveit::planning_interface::MoveGroupInterface::Plan plan;
    plan.trajectory_ = trajectory;

    const bool executed = move_group_->execute(plan) == moveit::core::MoveItErrorCode::SUCCESS;
    if (!executed) {
      RCLCPP_ERROR(get_logger(), "Execution failed during stage: %s", stage_name.c_str());
      return false;
    }

    std::this_thread::sleep_for(300ms);
    return true;
  }

  bool moveToNamedTarget(
    const std::shared_ptr<moveit::planning_interface::MoveGroupInterface>& group,
    const std::string& named_target)
  {
    group->setStartStateToCurrentState();
    group->setNamedTarget(named_target);

    moveit::planning_interface::MoveGroupInterface::Plan plan;
    const bool planned = group->plan(plan) == moveit::core::MoveItErrorCode::SUCCESS;

    if (!planned) {
      RCLCPP_ERROR(get_logger(), "Planning failed for named target: %s", named_target.c_str());
      return false;
    }

    return group->execute(plan) == moveit::core::MoveItErrorCode::SUCCESS;
  }

  bool commandGripper(double position)
  {
    if (!gripper_action_client_->wait_for_action_server(10s)) {
      RCLCPP_ERROR(get_logger(), "Gripper action server is not available.");
      return false;
    }

    GripperCommand::Goal goal;
    goal.command.position = position;
    goal.command.max_effort = -1.0;

    auto goal_future = gripper_action_client_->async_send_goal(goal);
    if (goal_future.wait_for(10s) != std::future_status::ready) {
      RCLCPP_ERROR(get_logger(), "Timed out while sending gripper goal.");
      return false;
    }

    const auto goal_handle = goal_future.get();
    if (!goal_handle) {
      RCLCPP_ERROR(get_logger(), "Gripper goal was rejected.");
      return false;
    }

    auto result_future = gripper_action_client_->async_get_result(goal_handle);
    if (result_future.wait_for(15s) != std::future_status::ready) {
      RCLCPP_ERROR(get_logger(), "Timed out while waiting for gripper result.");
      return false;
    }

    const auto result = result_future.get();
    if (result.code != rclcpp_action::ResultCode::SUCCEEDED) {
      RCLCPP_ERROR(get_logger(), "Gripper action did not succeed.");
      return false;
    }

    std::this_thread::sleep_for(300ms);
    return true;
  }

  bool attachObjectAtPose(const std::vector<double>& world_position)
  {
    auto object = makeBoxObject(object_id_, world_position, object_size_);
    if (!planning_scene_interface_.applyCollisionObject(object)) {
      RCLCPP_ERROR(get_logger(), "Failed to re-add object before attaching.");
      return false;
    }

    std::this_thread::sleep_for(300ms);

    const auto* hand_jmg = move_group_->getRobotModel()->getJointModelGroup(hand_group_);
    const auto touch_links = hand_jmg ? hand_jmg->getLinkModelNamesWithCollisionGeometry() : std::vector<std::string>{};

    if (!move_group_->attachObject(object_id_, hand_frame_, touch_links)) {
      RCLCPP_ERROR(get_logger(), "Failed to attach object to robot.");
      return false;
    }

    std::this_thread::sleep_for(300ms);
    return true;
  }

  bool detachObjectAtPose(const std::vector<double>& world_position)
  {
    if (!move_group_->detachObject(object_id_)) {
      RCLCPP_ERROR(get_logger(), "Failed to detach object from robot.");
      return false;
    }

    std::this_thread::sleep_for(300ms);

    auto object = makeBoxObject(object_id_, world_position, object_size_);
    if (!planning_scene_interface_.applyCollisionObject(object)) {
      RCLCPP_ERROR(get_logger(), "Failed to place detached object back into planning scene.");
      return false;
    }

    std::this_thread::sleep_for(300ms);
    return true;
  }

  void setAllowedCollision(const std::string& first, const std::string& second, bool allowed)
  {
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

    const auto all_links = move_group_->getRobotModel()->getLinkModelNamesWithCollisionGeometry();
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

  std::shared_ptr<moveit::planning_interface::MoveGroupInterface> move_group_;
  std::shared_ptr<moveit::planning_interface::MoveGroupInterface> right_move_group_;
  moveit::planning_interface::PlanningSceneInterface planning_scene_interface_;
  rclcpp_action::Client<GripperCommand>::SharedPtr gripper_action_client_;
  rclcpp::Client<moveit_msgs::srv::GetPlanningScene>::SharedPtr planning_scene_client_;

  std::string world_frame_;
  std::string object_id_;
  std::string table_id_;
  std::string arm_group_;
  std::string hand_group_;
  std::string hand_frame_;
  std::string home_pose_name_;
  std::string right_arm_group_;
  std::string right_home_pose_name_;
  bool ignore_right_arm_collisions_{};

  std::vector<double> source_position_;
  std::vector<double> target_position_;
  std::vector<double> object_size_;
  std::vector<double> table_position_;
  std::vector<double> table_size_;
  std::vector<double> source_position_offset_;
  std::vector<double> pick_yaw_candidates_;

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
  double place_yaw_{};
  double workspace_margin_xy_{};
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
