pragma solidity ^0.8.10;

interface BeefyVaultV6 {
    event Approval(address indexed owner, address indexed spender, uint256 value);
    event NewStratCandidate(address implementation);
    event OwnershipTransferred(address indexed previousOwner, address indexed newOwner);
    event Transfer(address indexed from, address indexed to, uint256 value);
    event UpgradeStrat(address implementation);

    function allowance(address owner, address spender) view external returns (uint256);
    function approvalDelay() view external returns (uint256);
    function approve(address spender, uint256 amount) external returns (bool);
    function available() view external returns (uint256);
    function balance() view external returns (uint256);
    function balanceOf(address account) view external returns (uint256);
    function decimals() view external returns (uint8);
    function decreaseAllowance(address spender, uint256 subtractedValue) external returns (bool);
    function deposit(uint256 _amount) external;
    function depositAll() external;
    function earn() external;
    function getPricePerFullShare() view external returns (uint256);
    function inCaseTokensGetStuck(address _token) external;
    function increaseAllowance(address spender, uint256 addedValue) external returns (bool);
    function name() view external returns (string memory);
    function owner() view external returns (address);
    function proposeStrat(address _implementation) external;
    function renounceOwnership() external;
    function stratCandidate() view external returns (address implementation, uint256 proposedTime);
    function strategy() view external returns (address);
    function symbol() view external returns (string memory);
    function totalSupply() view external returns (uint256);
    function transfer(address recipient, uint256 amount) external returns (bool);
    function transferFrom(address sender, address recipient, uint256 amount) external returns (bool);
    function transferOwnership(address newOwner) external;
    function upgradeStrat() external;
    function want() view external returns (address);
    function withdraw(uint256 _shares) external;
    function withdrawAll() external;
}
